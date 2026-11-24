package disk

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/buchgr/bazel-remote/v2/cache"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type proxyCheck struct {
	wg          *sync.WaitGroup
	digest      **pb.Digest
	ctx         context.Context
	onProxyMiss func()
}

var errMissingBlob = errors.New("a blob could not be found")

var errRequestCancelled = status.Error(codes.Canceled, "Request was cancelled")

const (
	// localContainsBatchSize moderates how long the cache lock is held by
	// findMissingLocalCAS.
	localContainsBatchSize = 20

	// defaultProxyContainsBatchSize bounds how many distinct, locally
	// missing digests are sent to the proxy backend in one existence
	// check. It is deliberately independent of localContainsBatchSize.
	defaultProxyContainsBatchSize = 100

	// maxProxyContainsWorkers bounds the number of concurrent proxy
	// existence checks, mirroring the previous fixed worker pool size.
	maxProxyContainsWorkers = 512
)

// proxyContainsErrors counts digest-level existence checks against the
// proxy backend that failed with an error. Such digests are still treated
// as missing for the current response, so this only makes the failure
// visible.
var proxyContainsErrors = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "bazel_remote_proxy_contains_errors_total",
	Help: "The number of digest existence checks against the proxy backend that failed with an " +
		"error. The affected digests are still reported as missing for the current response.",
})

// findMissingStats records the counters of a single FindMissingCasBlobs
// run, primarily useful for tests and diagnostics.
type findMissingStats struct {
	// inputCount is the number of digest entries in the original request,
	// counting duplicates.
	inputCount int

	// uniqueCount is the number of distinct digests, after de-duplication.
	uniqueCount int

	// localHits is the number of distinct digests found in the local disk
	// cache.
	localHits int

	// proxyBatches is the number of existence-check batches actually sent
	// to the proxy backend.
	proxyBatches int
}

// proxyEntry pairs a digest to check on the proxy with its index in the
// de-duplicated unique slice, so hits can be mirrored into the input slice.
type proxyEntry struct {
	uniqueIdx int
	digest    *pb.Digest
}

// Optimised implementation of FindMissingBlobs, which batches local index
// lookups and performs concurrent proxy lookups for local cache misses.
// Returns a slice with the blobs that are missing from the cache.
//
// Note that this modifies the input slice and returns a subset of it.
func (c *diskCache) FindMissingCasBlobs(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, error) {
	missing, _, err := c.FindMissingCasBlobsWithStats(ctx, blobs)
	return missing, err
}

// FindMissingCasBlobsWithStats behaves like FindMissingCasBlobs and
// additionally returns the counters for the run: the number of input
// entries (including duplicates), the number of distinct digests, how many
// were found locally and how many existence-check batches were sent to the
// proxy backend. The batch count is computed from the unique digests that
// remained missing after the local check, never from the raw request size.
func (c *diskCache) FindMissingCasBlobsWithStats(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, findMissingStats, error) {
	stats, err := c.findMissingCasBlobsInternalWithStats(ctx, blobs, false)
	if err != nil {
		return nil, stats, err
	}
	return filterNonNil(blobs), stats, nil
}

// Identifies local and proxy cache misses for blobs. Modifies the input `blobs` slice such that found
// blobs are replaced with nil, while the missing digests remain unchanged.
//
// When failFast is true and a blob could not be found in the local cache nor in the
// proxy back end, the search will immediately terminate and errMissingBlob will be returned. Given that the
// search is terminated early, the contents of blobs will only have partially been updated.
func (c *diskCache) findMissingCasBlobsInternal(ctx context.Context, blobs []*pb.Digest, failFast bool) error {
	_, err := c.findMissingCasBlobsInternalWithStats(ctx, blobs, failFast)
	return err
}

// findMissingCasBlobsInternalWithStats is the implementation shared by the
// public and fail-fast entry points; it returns the run statistics in
// addition to the error.
func (c *diskCache) findMissingCasBlobsInternalWithStats(ctx context.Context, blobs []*pb.Digest, failFast bool) (findMissingStats, error) {
	var cancelContextForFailFast context.CancelFunc = nil
	var cancelledDueToFailFast atomic.Bool

	if failFast && c.proxy != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		cancelContextForFailFast = func() {
			// Indicate that we were canceled so that we can fail fast.
			cancelledDueToFailFast.Store(true)
			cancel()
		}
	}

	// De-duplicate the request before any lookup: clients do send repeated
	// digests, and each unique digest must be checked at most once. Keep
	// track of every input slot that refers to each unique digest so that
	// hits are reflected at all of them.
	unique := make([]*pb.Digest, 0, len(blobs))
	inputSlots := make([][]int, 0, len(blobs))
	indexByHash := make(map[string]int, len(blobs))

	for i := range blobs {
		if blobs[i] == nil {
			continue
		}
		key := blobs[i].Hash
		if idx, ok := indexByHash[key]; ok {
			inputSlots[idx] = append(inputSlots[idx], i)
			continue
		}
		indexByHash[key] = len(unique)
		unique = append(unique, blobs[i])
		inputSlots = append(inputSlots, []int{i})
	}

	stats := findMissingStats{
		inputCount:  len(blobs),
		uniqueCount: len(unique),
	}

	// Batch local index lookups. A nil entry in unique means "found
	// locally". The local batch size is independent of the proxy batch
	// size.
	var chunk []*pb.Digest
	remaining := unique
	for len(remaining) > 0 {
		select {
		case <-ctx.Done():
			if cancelledDueToFailFast.Load() {
				return stats, errMissingBlob
			}
			return stats, errRequestCancelled
		default:
		}

		if len(remaining) <= localContainsBatchSize {
			chunk = remaining
			remaining = nil
		} else {
			chunk = remaining[:localContainsBatchSize]
			remaining = remaining[localContainsBatchSize:]
		}

		c.findMissingLocalCAS(chunk)
	}

	// Mirror local hits into every input slot, including duplicates.
	localHits := 0
	for i := range unique {
		if unique[i] == nil {
			localHits++
			for _, slot := range inputSlots[i] {
				blobs[slot] = nil
			}
		}
	}
	stats.localHits = localHits

	if c.proxy == nil {
		if failFast && len(unique) > localHits {
			// There are local cache misses, no proxy, and we are failing fast.
			return stats, errMissingBlob
		}
		return stats, nil
	}

	// Collect the unique digests that are still missing locally and which
	// are small enough to look up on the proxy backend. Digests over the
	// proxy size limit stay missing and, in fail-fast mode, terminate the
	// search immediately (as before).
	var toCheck []proxyEntry
	for i := range unique {
		if unique[i] == nil {
			continue
		}
		if unique[i].SizeBytes > c.maxProxyBlobSize {
			if failFast {
				return stats, errMissingBlob
			}
			continue
		}
		toCheck = append(toCheck, proxyEntry{uniqueIdx: i, digest: unique[i]})
	}

	if len(toCheck) == 0 {
		return stats, nil
	}

	// Cap concurrent proxy checks, including in-flight batched RPCs.
	sem := semaphore.NewWeighted(maxProxyContainsWorkers)
	var wg sync.WaitGroup

	batchSize := c.proxyContainsBatchSize
	if batchSize <= 0 {
		batchSize = defaultProxyContainsBatchSize
	}

	batchProxy, canBatch := c.proxy.(cache.BatchProxy)

	for start := 0; start < len(toCheck); start += batchSize {
		end := start + batchSize
		if end > len(toCheck) {
			end = len(toCheck)
		}
		batch := toCheck[start:end]

		select {
		case <-ctx.Done():
			if cancelledDueToFailFast.Load() {
				return stats, errMissingBlob
			}
			return stats, errRequestCancelled
		default:
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			if cancelledDueToFailFast.Load() {
				return stats, errMissingBlob
			}
			return stats, errRequestCancelled
		}

		wg.Add(1)
		stats.proxyBatches++

		go func(entries []proxyEntry) {
			defer wg.Done()
			defer sem.Release(1)

			select {
			case <-ctx.Done():
				return
			default:
			}

			if canBatch {
				c.checkProxyBatch(ctx, batchProxy, entries, blobs, inputSlots, cancelContextForFailFast)
			} else {
				c.checkProxyOneByOne(ctx, entries, blobs, inputSlots, cancelContextForFailFast)
			}
		}(batch)
	}

	// Adapt the waitgroup for select so a cancellation interrupts the wait.
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-ctx.Done():
		if cancelledDueToFailFast.Load() {
			return stats, errMissingBlob
		}
		return stats, errRequestCancelled
	case <-waitCh: // Everything in the waitgroup has finished.
	}

	return stats, nil
}

// markProxyHit records that the digest of the given unique index exists on
// the proxy backend, clearing it (and every duplicated input slot) from the
// missing set.
func (c *diskCache) markProxyHit(blobs []*pb.Digest, inputSlots [][]int, uniqueIdx int) {
	for _, slot := range inputSlots[uniqueIdx] {
		blobs[slot] = nil
	}
}

// checkProxyBatch handles one batched existence check against a BatchProxy
// backend and applies the results. Errors are treated as misses (while
// incrementing a dedicated counter); in fail-fast mode any miss cancels the
// remaining batches.
func (c *diskCache) checkProxyBatch(
	ctx context.Context,
	proxy cache.BatchProxy,
	entries []proxyEntry,
	blobs []*pb.Digest,
	inputSlots [][]int,
	onProxyMiss func(),
) {
	hashes := make([]string, len(entries))
	sizes := make([]int64, len(entries))
	for i, e := range entries {
		hashes[i] = e.digest.Hash
		sizes[i] = e.digest.SizeBytes
	}

	results := proxy.ContainsMany(ctx, hashes, sizes)

	// A broken backend must never shrink the response: if the number or
	// ordering of results is unusable, treat the whole batch as misses.
	if len(results) != len(entries) {
		proxyContainsErrors.Add(float64(len(entries)))
		log.Printf("ERROR: proxy batch contains returned %d results for %d digests",
			len(results), len(entries))
		c.proxyMissed(onProxyMiss)
		return
	}

	for i, res := range results {
		hash := entries[i].digest.Hash
		if res.Err != nil {
			proxyContainsErrors.Inc()
			log.Printf("ERROR: proxy contains check for %s failed: %v", hash, res.Err)
			c.proxyMissed(onProxyMiss)
			continue
		}
		if res.Exists {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", hash)
			c.markProxyHit(blobs, inputSlots, entries[i].uniqueIdx)
		} else {
			c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", hash)
			c.proxyMissed(onProxyMiss)
		}
	}
}

// checkProxyOneByOne retains the historical per-digest lookup for proxy
// backends that do not implement BatchProxy. The input order and fail-fast
// semantics match the batched path exactly.
func (c *diskCache) checkProxyOneByOne(
	ctx context.Context,
	entries []proxyEntry,
	blobs []*pb.Digest,
	inputSlots [][]int,
	onProxyMiss func(),
) {
	for _, e := range entries {
		select {
		case <-ctx.Done():
			c.accessLogger.Printf("GRPC CAS HEAD %s CANCELLED", e.digest.Hash)
			return
		default:
		}

		ok, _ := c.proxy.Contains(ctx, cache.CAS, e.digest.Hash, e.digest.SizeBytes)
		if ok {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", e.digest.Hash)
			c.markProxyHit(blobs, inputSlots, e.uniqueIdx)
		} else {
			c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", e.digest.Hash)
			c.proxyMissed(onProxyMiss)
		}
	}
}

// proxyMissed records a proxy miss and, in fail-fast mode, cancels the rest
// of the search.
func (c *diskCache) proxyMissed(onProxyMiss func()) {
	if onProxyMiss != nil {
		onProxyMiss()
	}
}

// Move all the non-nil items in the input slice to the
// start, and return the non-nil sub-slice.
func filterNonNil(blobs []*pb.Digest) []*pb.Digest {
	count := 0
	for i := 0; i < len(blobs); i++ {
		if blobs[i] != nil {
			blobs[count] = blobs[i]
			count++
		}
	}

	return blobs[:count]
}

// Set blobs that exist in the disk cache to nil, and return the number
// of missing blobs.
func (c *diskCache) findMissingLocalCAS(blobs []*pb.Digest) int {
	var key string
	missing := 0

	c.mu.Lock()

	for i := range blobs {
		if blobs[i].SizeBytes == 0 && blobs[i].Hash == emptySha256 {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", blobs[i].Hash)
			blobs[i] = nil
			continue
		}

		foundSize := int64(-1)
		key = cache.LookupKey(cache.CAS, blobs[i].Hash)
		item, listElem := c.lru.Get(key)
		if listElem != nil {
			foundSize = item.size
		}

		if listElem != nil && !isSizeMismatch(blobs[i].SizeBytes, foundSize) {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", blobs[i].Hash)
			blobs[i] = nil
		} else {
			missing++
		}
	}

	c.mu.Unlock()

	return missing
}

// containsWorker and proxyCheck are retained for tests that drive a
// single-digest existence check directly. Production lookups go through the
// batched/one-by-one paths above instead of a shared worker pool.
func (c *diskCache) containsWorker() {
	var ok bool
	for req := range c.containsQueue {
		if req.ctx != nil {
			select {
			case <-req.ctx.Done():
				// Fast-fail if the context has already been cancelled.
				c.accessLogger.Printf("GRPC CAS HEAD %s CANCELLED", (*req.digest).Hash)
				req.wg.Done()
				continue
			default:
			}
		}

		ok, _ = c.proxy.Contains(req.ctx, cache.CAS, (*req.digest).Hash, (*req.digest).SizeBytes)
		if ok {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", (*req.digest).Hash)
			// The blob exists on the proxy, remove it from the
			// list of missing blobs.
			*(req.digest) = nil
		} else {
			c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", (*req.digest).Hash)
			if req.onProxyMiss != nil {
				req.onProxyMiss()
			}
		}
		req.wg.Done()
	}
}
