package runtime

import (
	"internal/goos"
	"internal/runtime/atomic"
	"internal/runtime/sys"
	"unsafe"
)

const (
	scavengePercent = 1 // 1%

	retainExtraPercent = 10

	reduceExtraPercent = 5

	maxPagesPerPhysPage = maxPhysPageSize / pageSize

	scavengeCostRatio = 0.7 * (goos.IsDarwin + goos.IsIos)

	scavChunkHiOccFrac  = 0.96875
	scavChunkHiOccPages = uint16(scavChunkHiOccFrac * pallocChunkPages)
)

func heapRetained() uint64 {
	return gcController.heapInUse.load() + gcController.heapFree.load()
}

func gcPaceScavenger(memoryLimit int64, heapGoal, lastHeapGoal uint64) {
	assertWorldStoppedOrLockHeld(&mheap_.lock)

	memoryLimitGoal := uint64(float64(memoryLimit) * (1 - reduceExtraPercent/100.0))

	mappedReady := gcController.mappedReady.Load()

	if mappedReady <= memoryLimitGoal {
		scavenge.memoryLimitGoal.Store(^uint64(0))
	} else {
		scavenge.memoryLimitGoal.Store(memoryLimitGoal)
	}

	if lastHeapGoal == 0 {
		scavenge.gcPercentGoal.Store(^uint64(0))
		return
	}
	goalRatio := float64(heapGoal) / float64(lastHeapGoal)
	gcPercentGoal := uint64(float64(memstats.lastHeapInUse) * goalRatio)
	gcPercentGoal += gcPercentGoal / (1.0 / (retainExtraPercent / 100.0))
	gcPercentGoal = (gcPercentGoal + uint64(physPageSize) - 1) &^ (uint64(physPageSize) - 1)

	heapRetainedNow := heapRetained()

	if heapRetainedNow <= gcPercentGoal || heapRetainedNow-gcPercentGoal < uint64(physPageSize) {
		scavenge.gcPercentGoal.Store(^uint64(0))
	} else {
		scavenge.gcPercentGoal.Store(gcPercentGoal)
	}
}

var scavenge struct {
	gcPercentGoal atomic.Uint64

	memoryLimitGoal atomic.Uint64

	assistTime atomic.Int64

	backgroundTime atomic.Int64
}

const (
	startingScavSleepRatio = 0.001

	minScavWorkTime = 1e6
)

var scavenger scavengerState

type scavengerState struct {
	lock mutex

	g *g

	timer *timer

	sysmonWake atomic.Uint32

	parked bool

	printControllerReset bool

	targetCPUFraction float64

	sleepRatio float64

	sleepController piController

	controllerCooldown int64

	sleepStub func(n int64) int64

	scavenge func(n uintptr) (uintptr, int64)

	shouldStop func() bool

	gomaxprocs func() int32
}

func (s *scavengerState) init() {
	if s.g != nil {
		throw("scavenger state is already wired")
	}
	lockInit(&s.lock, lockRankScavenge)
	s.g = getg()

	s.timer = new(timer)
	f := func(s any, _ uintptr, _ int64) {
		s.(*scavengerState).wake()
	}
	s.timer.init(f, s)

	s.sleepController = piController{
		kp: 0.3375,
		ti: 3.2e6,
		tt: 1e9,

		min: 0.001,
		max: 1000.0,
	}
	s.sleepRatio = startingScavSleepRatio

	if s.scavenge == nil {
		s.scavenge = func(n uintptr) (uintptr, int64) {
			start := nanotime()
			r := mheap_.pages.scavenge(n, nil, false)
			end := nanotime()
			if start >= end {
				return r, 0
			}
			scavenge.backgroundTime.Add(end - start)
			return r, end - start
		}
	}
	if s.shouldStop == nil {
		s.shouldStop = func() bool {
			return heapRetained() <= scavenge.gcPercentGoal.Load() &&
				gcController.mappedReady.Load() <= scavenge.memoryLimitGoal.Load()
		}
	}
	if s.gomaxprocs == nil {
		s.gomaxprocs = func() int32 {
			return gomaxprocs
		}
	}
}

func (s *scavengerState) park() {
	lock(&s.lock)
	if getg() != s.g {
		throw("tried to park scavenger from another goroutine")
	}
	s.parked = true
	goparkunlock(&s.lock, waitReasonGCScavengeWait, traceBlockSystemGoroutine, 2)
}

func (s *scavengerState) ready() {
	s.sysmonWake.Store(1)
}

func (s *scavengerState) wake() {
	lock(&s.lock)
	if s.parked {
		s.sysmonWake.Store(0)

		s.parked = false

		var list gList
		list.push(s.g)
		injectglist(&list)
	}
	unlock(&s.lock)
}

func (s *scavengerState) sleep(worked float64) {
	lock(&s.lock)
	if getg() != s.g {
		throw("tried to sleep scavenger from another goroutine")
	}

	if worked < minScavWorkTime {
		worked = minScavWorkTime
	}

	worked *= 1 + scavengeCostRatio

	sleepTime := int64(worked / s.sleepRatio)

	var slept int64
	if s.sleepStub == nil {
		start := nanotime()
		s.timer.reset(start+sleepTime, 0)

		s.parked = true
		goparkunlock(&s.lock, waitReasonSleep, traceBlockSleep, 2)

		slept = nanotime() - start

		lock(&s.lock)
		s.timer.stop()
		unlock(&s.lock)
	} else {
		unlock(&s.lock)
		slept = s.sleepStub(sleepTime)
	}

	if s.controllerCooldown > 0 {
		t := slept + int64(worked)
		if t > s.controllerCooldown {
			s.controllerCooldown = 0
		} else {
			s.controllerCooldown -= t
		}
		return
	}

	idealFraction := float64(scavengePercent) / 100.0

	cpuFraction := worked / ((float64(slept) + worked) * float64(s.gomaxprocs()))

	var ok bool
	s.sleepRatio, ok = s.sleepController.next(cpuFraction, idealFraction, float64(slept)+worked)
	if !ok {
		s.sleepRatio = startingScavSleepRatio
		s.controllerCooldown = 5e9

		s.controllerFailed()
	}
}

func (s *scavengerState) controllerFailed() {
	lock(&s.lock)
	s.printControllerReset = true
	unlock(&s.lock)
}

func (s *scavengerState) run() (released uintptr, worked float64) {
	lock(&s.lock)
	if getg() != s.g {
		throw("tried to run scavenger from another goroutine")
	}
	unlock(&s.lock)

	for worked < minScavWorkTime {
		if s.shouldStop() {
			break
		}

		const scavengeQuantum = 64 << 10

		r, duration := s.scavenge(scavengeQuantum)

		const approxWorkedNSPerPhysicalPage = 10e3
		if duration == 0 {
			worked += approxWorkedNSPerPhysicalPage * float64(r/physPageSize)
		} else {
			worked += float64(duration)
		}
		released += r

		if r < scavengeQuantum {
			break
		}
		if faketime != 0 {
			break
		}
	}
	if released > 0 && released < physPageSize {
		throw("released less than one physical page of memory")
	}
	return
}

func bgscavenge(c chan int) {
	scavenger.init()

	c <- 1
	scavenger.park()

	for {
		released, workTime := scavenger.run()
		if released == 0 {
			scavenger.park()
			continue
		}
		mheap_.pages.scav.releasedBg.Add(released)
		scavenger.sleep(workTime)
	}
}

func (p *pageAlloc) scavenge(nbytes uintptr, shouldStop func() bool, force bool) uintptr {
	released := uintptr(0)
	for released < nbytes {
		ci, pageIdx := p.scav.index.find(force)
		if ci == 0 {
			break
		}
		systemstack(func() {
			released += p.scavengeOne(ci, pageIdx, nbytes-released)
		})
		if shouldStop != nil && shouldStop() {
			break
		}
	}
	return released
}

func printScavTrace(releasedBg, releasedEager uintptr, forced bool) {
	assertLockHeld(&scavenger.lock)

	printlock()
	print("scav ",
		releasedBg>>10, " KiB work (bg), ",
		releasedEager>>10, " KiB work (eager), ",
		gcController.heapReleased.load()>>10, " KiB now, ",
		(gcController.heapInUse.load()*100)/heapRetained(), "% util",
	)
	if forced {
		print(" (forced)")
	} else if scavenger.printControllerReset {
		print(" [controller reset]")
		scavenger.printControllerReset = false
	}
	println()
	printunlock()
}

func (p *pageAlloc) scavengeOne(ci chunkIdx, searchIdx uint, max uintptr) uintptr {
	maxPages := max / pageSize
	if max%pageSize != 0 {
		maxPages++
	}

	minPages := physPageSize / pageSize
	if minPages < 1 {
		minPages = 1
	}

	lock(p.mheapLock)
	if p.summary[len(p.summary)-1][ci].max() >= uint(minPages) {
		base, npages := p.chunkOf(ci).findScavengeCandidate(searchIdx, minPages, maxPages)

		if npages != 0 {
			addr := chunkBase(ci) + uintptr(base)*pageSize

			p.chunkOf(ci).allocRange(base, npages)
			p.update(addr, uintptr(npages), true, true)

			unlock(p.mheapLock)

			if !p.test {
				sysUnused(unsafe.Pointer(addr), uintptr(npages)*pageSize)

				nbytes := int64(npages * pageSize)
				gcController.heapReleased.add(nbytes)
				gcController.heapFree.add(-nbytes)

				stats := memstats.heapStats.acquire()
				atomic.Xaddint64(&stats.committed, -nbytes)
				atomic.Xaddint64(&stats.released, nbytes)
				memstats.heapStats.release()
			}

			lock(p.mheapLock)
			if b := (offAddr{addr}); b.lessThan(p.searchAddr) {
				p.searchAddr = b
			}
			p.chunkOf(ci).free(base, npages)
			p.update(addr, uintptr(npages), true, false)

			p.chunkOf(ci).scavenged.setRange(base, npages)
			unlock(p.mheapLock)

			return uintptr(npages) * pageSize
		}
	}
	p.scav.index.setEmpty(ci)
	unlock(p.mheapLock)

	return 0
}

func fillAligned(x uint64, m uint) uint64 {
	apply := func(x uint64, c uint64) uint64 {
		return ^((((x & c) + c) | x) | c)
	}
	switch m {
	case 1:
		return x
	case 2:
		x = apply(x, 0x5555555555555555)
	case 4:
		x = apply(x, 0x7777777777777777)
	case 8:
		x = apply(x, 0x7f7f7f7f7f7f7f7f)
	case 16:
		x = apply(x, 0x7fff7fff7fff7fff)
	case 32:
		x = apply(x, 0x7fffffff7fffffff)
	case 64:
		x = apply(x, 0x7fffffffffffffff)
	default:
		throw("bad m value")
	}
	return ^((x - (x >> (m - 1))) | x)
}

func (m *pallocData) findScavengeCandidate(searchIdx uint, minimum, max uintptr) (uint, uint) {
	if minimum&(minimum-1) != 0 || minimum == 0 {
		print("runtime: min = ", minimum, "\n")
		throw("min must be a non-zero power of 2")
	} else if minimum > maxPagesPerPhysPage {
		print("runtime: min = ", minimum, "\n")
		throw("min too large")
	}
	if max == 0 {
		max = minimum
	} else {
		max = alignUp(max, minimum)
	}

	i := int(searchIdx / 64)
	for ; i >= 0; i-- {
		x := fillAligned(m.scavenged[i]|m.pallocBits[i], uint(minimum))
		if x != ^uint64(0) {
			break
		}
	}
	if i < 0 {
		return 0, 0
	}
	x := fillAligned(m.scavenged[i]|m.pallocBits[i], uint(minimum))
	z1 := uint(sys.LeadingZeros64(^x))
	run, end := uint(0), uint(i)*64+(64-z1)
	if x<<z1 != 0 {
		run = uint(sys.LeadingZeros64(x << z1))
	} else {
		run = 64 - z1
		for j := i - 1; j >= 0; j-- {
			x := fillAligned(m.scavenged[j]|m.pallocBits[j], uint(minimum))
			run += uint(sys.LeadingZeros64(x))
			if x != 0 {
				break
			}
		}
	}

	size := min(run, uint(max))
	start := end - size

	if physHugePageSize > pageSize && physHugePageSize > physPageSize {
		pagesPerHugePage := physHugePageSize / pageSize
		hugePageAbove := uint(alignUp(uintptr(start), pagesPerHugePage))

		if hugePageAbove <= end {
			hugePageBelow := uint(alignDown(uintptr(start), pagesPerHugePage))

			if hugePageBelow >= end-run {
				size = size + (start - hugePageBelow)
				start = hugePageBelow
			}
		}
	}
	return start, size
}

type scavengeIndex struct {
	chunks     []atomicScavChunkData
	min, max   atomic.Uintptr
	minHeapIdx atomic.Uintptr

	searchAddrBg    atomicOffAddr
	searchAddrForce atomicOffAddr

	freeHWM offAddr

	gen uint32

	test bool
}

func (s *scavengeIndex) init(test bool, sysStat *sysMemStat) uintptr {
	s.searchAddrBg.Clear()
	s.searchAddrForce.Clear()
	s.freeHWM = minOffAddr
	s.test = test
	return s.sysInit(test, sysStat)
}

func (s *scavengeIndex) grow(base, limit uintptr, sysStat *sysMemStat) uintptr {
	minHeapIdx := s.minHeapIdx.Load()
	if baseIdx := uintptr(chunkIndex(base)); minHeapIdx == 0 || baseIdx < minHeapIdx {
		s.minHeapIdx.Store(baseIdx)
	}
	return s.sysGrow(base, limit, sysStat)
}

func (s *scavengeIndex) find(force bool) (chunkIdx, uint) {
	cursor := &s.searchAddrBg
	if force {
		cursor = &s.searchAddrForce
	}
	searchAddr, marked := cursor.Load()
	if searchAddr == minOffAddr.addr() {
		return 0, 0
	}

	gen := s.gen
	min := chunkIdx(s.minHeapIdx.Load())
	start := chunkIndex(searchAddr)
	for i := start; i >= min; i-- {
		if !s.chunks[i].load().shouldScavenge(gen, force) {
			continue
		}
		if i == start {
			return i, chunkPageIndex(searchAddr)
		}
		newSearchAddr := chunkBase(i) + pallocChunkBytes - pageSize
		if marked {
			cursor.StoreUnmark(searchAddr, newSearchAddr)
		} else {
			cursor.StoreMin(newSearchAddr)
		}
		return i, pallocChunkPages - 1
	}
	cursor.Clear()
	return 0, 0
}

func (s *scavengeIndex) alloc(ci chunkIdx, npages uint) {
	sc := s.chunks[ci].load()
	sc.alloc(npages, s.gen)
	s.chunks[ci].store(sc)
}

func (s *scavengeIndex) free(ci chunkIdx, page, npages uint) {
	sc := s.chunks[ci].load()
	sc.free(npages, s.gen)
	s.chunks[ci].store(sc)

	addr := chunkBase(ci) + uintptr(page+npages-1)*pageSize
	if s.freeHWM.lessThan(offAddr{addr}) {
		s.freeHWM = offAddr{addr}
	}
	searchAddr, _ := s.searchAddrForce.Load()
	if (offAddr{searchAddr}).lessThan(offAddr{addr}) {
		s.searchAddrForce.StoreMarked(addr)
	}
}

func (s *scavengeIndex) nextGen() {
	s.gen++
	searchAddr, _ := s.searchAddrBg.Load()
	if (offAddr{searchAddr}).lessThan(s.freeHWM) {
		s.searchAddrBg.StoreMarked(s.freeHWM.addr())
	}
	s.freeHWM = minOffAddr
}

func (s *scavengeIndex) setEmpty(ci chunkIdx) {
	val := s.chunks[ci].load()
	val.setEmpty()
	s.chunks[ci].store(val)
}

type atomicScavChunkData struct {
	value atomic.Uint64
}

func (sc *atomicScavChunkData) load() scavChunkData {
	return unpackScavChunkData(sc.value.Load())
}

func (sc *atomicScavChunkData) store(ssc scavChunkData) {
	sc.value.Store(ssc.pack())
}

type scavChunkData struct {
	inUse uint16

	lastInUse uint16

	gen uint32

	scavChunkFlags
}

func unpackScavChunkData(sc uint64) scavChunkData {
	return scavChunkData{
		inUse:          uint16(sc),
		lastInUse:      uint16(sc>>16) & scavChunkInUseMask,
		gen:            uint32(sc >> 32),
		scavChunkFlags: scavChunkFlags(uint8(sc>>(16+logScavChunkInUseMax)) & scavChunkFlagsMask),
	}
}

func (sc scavChunkData) pack() uint64 {
	return uint64(sc.inUse) |
		(uint64(sc.lastInUse) << 16) |
		(uint64(sc.scavChunkFlags) << (16 + logScavChunkInUseMax)) |
		(uint64(sc.gen) << 32)
}

const (
	scavChunkHasFree scavChunkFlags = 1 << iota

	scavChunkMaxFlags  = 6
	scavChunkFlagsMask = (1 << scavChunkMaxFlags) - 1

	logScavChunkInUseMax = logPallocChunkPages + 1
	scavChunkInUseMask   = (1 << logScavChunkInUseMax) - 1
)

type scavChunkFlags uint8

func (sc *scavChunkFlags) isEmpty() bool {
	return (*sc)&scavChunkHasFree == 0
}

func (sc *scavChunkFlags) setEmpty() {
	*sc &^= scavChunkHasFree
}

func (sc *scavChunkFlags) setNonEmpty() {
	*sc |= scavChunkHasFree
}

func (sc scavChunkData) shouldScavenge(currGen uint32, force bool) bool {
	if sc.isEmpty() {
		return false
	}
	if force {
		return true
	}
	if sc.gen == currGen {
		return sc.inUse < scavChunkHiOccPages && sc.lastInUse < scavChunkHiOccPages
	}
	return sc.inUse < scavChunkHiOccPages
}

func (sc *scavChunkData) alloc(npages uint, newGen uint32) {
	if uint(sc.inUse)+npages > pallocChunkPages {
		print("runtime: inUse=", sc.inUse, " npages=", npages, "\n")
		throw("too many pages allocated in chunk?")
	}
	if sc.gen != newGen {
		sc.lastInUse = sc.inUse
		sc.gen = newGen
	}
	sc.inUse += uint16(npages)
	if sc.inUse == pallocChunkPages {
		sc.setEmpty()
	}
}

func (sc *scavChunkData) free(npages uint, newGen uint32) {
	if uint(sc.inUse) < npages {
		print("runtime: inUse=", sc.inUse, " npages=", npages, "\n")
		throw("allocated pages below zero?")
	}
	if sc.gen != newGen {
		sc.lastInUse = sc.inUse
		sc.gen = newGen
	}
	sc.inUse -= uint16(npages)
	sc.setNonEmpty()
}

type piController struct {
	kp float64
	ti float64
	tt float64

	min, max float64

	errIntegral float64

	errOverflow   bool
	inputOverflow bool
}

func (c *piController) next(input, setpoint, period float64) (float64, bool) {
	prop := c.kp * (setpoint - input)
	rawOutput := prop + c.errIntegral

	output := rawOutput
	if isInf(output) || isNaN(output) {
		c.reset()
		c.inputOverflow = true
		return c.min, false
	}
	if output < c.min {
		output = c.min
	} else if output > c.max {
		output = c.max
	}

	if c.ti != 0 && c.tt != 0 {
		c.errIntegral += (c.kp*period/c.ti)*(setpoint-input) + (period/c.tt)*(output-rawOutput)
		if isInf(c.errIntegral) || isNaN(c.errIntegral) {
			c.reset()
			c.errOverflow = true
			return c.min, false
		}
	}
	return output, true
}

func (c *piController) reset() {
	c.errIntegral = 0
}
