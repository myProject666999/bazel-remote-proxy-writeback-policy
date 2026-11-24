package disk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
)

// digestSet builds n distinct random digests.
func digestSet(t *testing.T, n int, size int64) []*pb.Digest {
	t.Helper()
	out := make([]*pb.Digest, n)
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		_, d := testutils.RandomDataAndDigest(size)
		if seen[d.Hash] {
			i--
			continue
		}
		seen[d.Hash] = true
		out[i] = &d
	}
	return out
}

// runFindMissing is the shared assertion used for both backend shapes. It
// returns the missing hashes and the fake proxy counters.
func runFindMissing(t *testing.T, proxy *recordingProxy, blobs []*pb.Digest,
	batchSize int) (missingHashes map[string]bool, containsCalls, containsManyCalls, containsDigests int) {

	t.Helper()
	opts := []Option{}
	if batchSize > 0 {
		opts = append(opts, WithProxyContainsBatchSize(batchSize))
	}
	c := newDiskCacheWithProxy(t, proxy.asProxy(), opts...)

	missing, err := c.FindMissingCasBlobs(context.Background(), blobs)
	if err != nil {
		t.Fatal(err)
	}
	missingHashes = map[string]bool{}
	for _, d := range missing {
		missingHashes[d.Hash] = true
	}
	containsCalls, containsManyCalls, containsDigests = proxy.containsCalls, proxy.containsManyCalls, proxy.containsDigests
	return missingHashes, containsCalls, containsManyCalls, containsDigests
}

// TestFindMissingDedupAndBatch covers the table of query scenarios against
// both a batched and a one-by-one backend, asserting identical results.
func TestFindMissingDedupAndBatch(t *testing.T) {
	t.Parallel()

	base := digestSet(t, 6, 100)
	d0, d1, d2, d3, d4, d5 := base[0], base[1], base[2], base[3], base[4], base[5]

	// Backend "existing" sets vary per case.
	cases := []struct {
		name string
		// blobs sent in the request (duplicates allowed).
		blobs []*pb.Digest
		// hashes present on the proxy backend.
		backendHas map[string]bool
		// hashes expected to be reported missing.
		wantMissing map[string]bool
		// expected number of unique digests reaching the backend.
		wantBackendDigests int
		// expected number of batches for the BatchProxy backend.
		wantBatches int
		batchSize   int
	}{
		{
			name:               "duplicates collapsed",
			blobs:              []*pb.Digest{d0, d0, d1, d1, d1, d2},
			backendHas:         map[string]bool{},
			wantMissing:        map[string]bool{d0.Hash: true, d1.Hash: true, d2.Hash: true},
			wantBackendDigests: 3,
			wantBatches:        1,
			batchSize:          10,
		},
		{
			name:               "all hit",
			blobs:              []*pb.Digest{d0, d1, d2},
			backendHas:         map[string]bool{d0.Hash: true, d1.Hash: true, d2.Hash: true},
			wantMissing:        map[string]bool{},
			wantBackendDigests: 3,
			wantBatches:        1,
			batchSize:          10,
		},
		{
			name:               "all missing",
			blobs:              []*pb.Digest{d0, d1, d2},
			backendHas:         map[string]bool{},
			wantMissing:        map[string]bool{d0.Hash: true, d1.Hash: true, d2.Hash: true},
			wantBackendDigests: 3,
			wantBatches:        1,
			batchSize:          10,
		},
		{
			name:               "partial missing",
			blobs:              []*pb.Digest{d0, d1, d2, d3},
			backendHas:         map[string]bool{d1.Hash: true, d3.Hash: true},
			wantMissing:        map[string]bool{d0.Hash: true, d2.Hash: true},
			wantBackendDigests: 4,
			wantBatches:        1,
			batchSize:          10,
		},
		{
			name:               "batch splitting",
			blobs:              []*pb.Digest{d0, d1, d2, d3, d4},
			backendHas:         map[string]bool{},
			wantMissing:        map[string]bool{d0.Hash: true, d1.Hash: true, d2.Hash: true, d3.Hash: true, d4.Hash: true},
			wantBackendDigests: 5,
			wantBatches:        3, // batch size 2 -> ceil(5/2)
			batchSize:          2,
		},
		{
			name:               "duplicates plus splitting",
			blobs:              []*pb.Digest{d0, d0, d1, d2, d2, d3, d4, d5},
			backendHas:         map[string]bool{d2.Hash: true},
			wantMissing:        map[string]bool{d0.Hash: true, d1.Hash: true, d3.Hash: true, d4.Hash: true, d5.Hash: true},
			wantBackendDigests: 6,
			wantBatches:        2, // batch size 3 -> ceil(6/3)
			batchSize:          3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Run once against each backend shape and require identical
			// missing sets.
			var batchedMissing, singleMissing map[string]bool

			// Batched backend.
			bp := newRecordingProxy(true)
			bp.existing = tc.backendHas
			batchedMissing, bSingle, bMany, bDigests := runFindMissing(t, bp, cloneBlobs(tc.blobs), tc.batchSize)
			if bSingle != 0 {
				t.Fatalf("batched backend: expected 0 Contains calls, got %d", bSingle)
			}
			if bMany != tc.wantBatches {
				t.Fatalf("batched backend: expected %d ContainsMany calls, got %d",
					tc.wantBatches, bMany)
			}
			if bDigests != 0 {
				t.Fatalf("batched backend: expected 0 one-by-one digests, got %d", bDigests)
			}

			// One-by-one backend (does not implement BatchProxy).
			sp := newRecordingProxy(false)
			sp.existing = tc.backendHas
			singleMissing, sCalls, sMany, sDigests := runFindMissing(t, sp, cloneBlobs(tc.blobs), tc.batchSize)
			if sMany != 0 {
				t.Fatalf("single backend: expected 0 ContainsMany calls, got %d", sMany)
			}
			if sCalls != tc.wantBackendDigests || sDigests != tc.wantBackendDigests {
				t.Fatalf("single backend: expected %d Contains calls/digests, got %d/%d",
					tc.wantBackendDigests, sCalls, sDigests)
			}

			if !sameSet(batchedMissing, tc.wantMissing) {
				t.Fatalf("batched missing set %v != expected %v", batchedMissing, tc.wantMissing)
			}
			if !sameSet(singleMissing, tc.wantMissing) {
				t.Fatalf("single missing set %v != expected %v", singleMissing, tc.wantMissing)
			}
			if !sameSet(batchedMissing, singleMissing) {
				t.Fatalf("backend shapes disagree: %v vs %v", batchedMissing, singleMissing)
			}
		})
	}
}

// TestFindMissingLocalHitsSkipBackend verifies locally present digests never
// reach the backend and are correctly de-duplicated.
func TestFindMissingLocalHitsSkipBackend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	proxy := newRecordingProxy(true)
	c := newDiskCacheWithProxy(t, proxy.asProxy(), WithProxyContainsBatchSize(10))

	data, digest := testutils.RandomDataAndDigest(100)
	if err := c.Put(ctx, cache.CAS, digest.Hash, digest.SizeBytes, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// Repeat the locally-present digest; add one genuinely missing digest.
	_, missing := testutils.RandomDataAndDigest(200)
	blobs := []*pb.Digest{&digest, &digest, &missing}

	got, _, many, _ := runFindMissingWithCache(t, c, blobs)
	if many != 1 {
		t.Fatalf("expected exactly 1 backend batch (only the missing digest), got %d", many)
	}
	if len(got) != 1 || got[0].Hash != missing.Hash {
		t.Fatalf("expected only %s missing, got %v", missing.Hash, got)
	}
}

// TestFindMissingStats verifies the four run counters and, critically, that
// batch counts derive from unique digests after the local check rather than
// from the raw (duplicate-bearing) request size.
func TestFindMissingStats(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	proxy := newRecordingProxy(true)
	c := newDiskCacheWithProxy(t, proxy.asProxy(), WithProxyContainsBatchSize(2))

	// Two locally present digests.
	localData1, local1 := testutils.RandomDataAndDigest(100)
	localData2, local2 := testutils.RandomDataAndDigest(100)
	for _, p := range []struct {
		d *pb.Digest
		b []byte
	}{{&local1, localData1}, {&local2, localData2}} {
		if err := c.Put(ctx, cache.CAS, p.d.Hash, p.d.SizeBytes, bytes.NewReader(p.b)); err != nil {
			t.Fatal(err)
		}
	}

	// Three unique backend-checked digests, none local.
	remote1 := digestSet(t, 3, 100)

	// Six input slots: local1 twice, local2 once, and the three remote
	// digests (one repeated). Unique count is therefore five.
	blobs := []*pb.Digest{
		&local1, &local1, // local hit, duplicated
		&local2,                                        // local hit
		remote1[0], remote1[1], remote1[2], remote1[0], // remote, last is a duplicate
	}

	_, stats, err := c.FindMissingCasBlobsWithStats(ctx, cloneBlobs(blobs))
	if err != nil {
		t.Fatal(err)
	}

	if stats.inputCount != 7 {
		t.Fatalf("inputCount: expected 7 raw entries, got %d", stats.inputCount)
	}
	if stats.uniqueCount != 5 {
		t.Fatalf("uniqueCount: expected 5 distinct digests, got %d", stats.uniqueCount)
	}
	if stats.localHits != 2 {
		t.Fatalf("localHits: expected 2, got %d", stats.localHits)
	}
	// Three unique digests reach the backend with a batch size of two =>
	// ceil(3/2) = 2 batches. The duplicate remote1[0] must not add a batch.
	if stats.proxyBatches != 2 {
		t.Fatalf("proxyBatches: expected 2 (from 3 unique digests), got %d", stats.proxyBatches)
	}
}

// TestFindMissingStatsNoProxy verifies the counters are still populated when
// there is no proxy backend (proxyBatches stays zero).
func TestFindMissingStatsNoProxy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cacheDir := tempDir(t)
	t.Cleanup(func() { _ = os.RemoveAll(cacheDir) })
	c, err := New(cacheDir, 100*1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	dc := c.(*diskCache)

	data, digest := testutils.RandomDataAndDigest(100)
	if err := dc.Put(ctx, cache.CAS, digest.Hash, digest.SizeBytes, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	_, other := testutils.RandomDataAndDigest(200)

	_, stats, err := dc.FindMissingCasBlobsWithStats(ctx,
		[]*pb.Digest{&digest, &digest, &other})
	if err != nil {
		t.Fatal(err)
	}
	if stats.inputCount != 3 || stats.uniqueCount != 2 || stats.localHits != 1 || stats.proxyBatches != 0 {
		t.Fatalf("unexpected stats without proxy: %+v", stats)
	}
}

// TestFindMissingBackendErrorTreatedAsMiss verifies that a backend error
// never produces a false "exists": every digest is still reported missing.
// For a BatchProxy the per-digest error is visible, so the disk layer also
// increments the error counter and logs the reason. The plain cache.Proxy
// Contains interface cannot transport errors, so for that shape the disk
// layer can only see a miss (the counter is therefore not incrementable
// there); the response must still be identical.
func TestFindMissingBackendErrorTreatedAsMiss(t *testing.T) {
	t.Parallel()

	base := digestSet(t, 3, 100)
	injected := errors.New("backend unavailable")

	for _, batched := range []bool{true, false} {
		name := "batched"
		if !batched {
			name = "one-by-one"
		}
		t.Run(name, func(t *testing.T) {
			proxy := newRecordingProxy(batched)
			proxy.containsErr = injected
			proxy.existing[base[0].Hash] = true // ignored because of the error

			before := testutil.ToFloat64(proxyContainsErrors)
			missing, _, _, _ := runFindMissing(t, proxy, cloneBlobs(base), 10)
			after := testutil.ToFloat64(proxyContainsErrors)

			if len(missing) != 3 {
				t.Fatalf("errored backend must report all digests missing, got %d", len(missing))
			}

			if batched {
				if after-before != 3 {
					t.Fatalf("batched backend: expected 3 error counter increments, got %v", after-before)
				}
			}
		})
	}
}

// TestFindMissingFailFastCancel verifies that a single miss in fail-fast mode
// cancels the remaining backend checks.
func TestFindMissingFailFastCancel(t *testing.T) {
	t.Parallel()

	// Enough digests and small batches to guarantee several in-flight batches.
	digests := digestSet(t, 20, 100)
	proxy := newRecordingProxy(true)
	// Everything is missing on the backend.
	gate := make(chan struct{})
	proxy.containsDelay = gate

	c := newDiskCacheWithProxy(t, proxy.asProxy(), WithProxyContainsBatchSize(2))

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.findMissingCasBlobsInternal(context.Background(), cloneBlobs(digests), true)
	}()

	// Release the blocked backend calls; the first miss must trigger
	// fail-fast cancellation.
	close(gate)

	err := <-errCh
	if !errors.Is(err, errMissingBlob) {
		t.Fatalf("expected errMissingBlob, got %v", err)
	}
}

// helpers

func runFindMissingWithCache(t *testing.T, c *diskCache, blobs []*pb.Digest) (
	[]*pb.Digest, int, int, int) {

	t.Helper()
	missing, err := c.FindMissingCasBlobs(context.Background(), blobs)
	if err != nil {
		t.Fatal(err)
	}
	return missing, c.proxy.(*recordingProxy).containsCalls,
		c.proxy.(*recordingProxy).containsManyCalls,
		c.proxy.(*recordingProxy).containsDigests
}

func cloneBlobs(in []*pb.Digest) []*pb.Digest {
	out := make([]*pb.Digest, len(in))
	copy(out, in)
	return out
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
