package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// recordingProxy is a cache.Proxy that records every Put and Contains call. It
// optionally implements cache.BatchProxy. Delays and per-call errors can be
// injected.
type recordingProxy struct {
	mu sync.Mutex

	// Existing hashes (CAS only).
	existing map[string]bool

	// Recorded uploads, in call order.
	puts []fakePut

	// Call counters.
	containsCalls     int
	containsManyCalls int
	containsDigests   int // total digests checked across all Contains calls

	// Controls.
	implementBatch bool
	containsErr    error // returned by every Contains/ContainsMany check
	containsDelay  chan struct{}
}

type fakePut struct {
	kind        cache.EntryKind
	hash        string
	logicalSize int64
}

func newRecordingProxy(implementBatch bool) *recordingProxy {
	return &recordingProxy{
		existing:       make(map[string]bool),
		implementBatch: implementBatch,
	}
}

// singleOnlyProxy exposes only the cache.Proxy methods of a recordingProxy,
// hiding its BatchProxy implementation so the disk cache takes the
// one-by-one fallback path. Call counting still happens on the inner proxy.
type singleOnlyProxy struct{ inner *recordingProxy }

func (s singleOnlyProxy) Put(ctx context.Context, kind cache.EntryKind, hash string,
	logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	s.inner.Put(ctx, kind, hash, logicalSize, sizeOnDisk, rc)
}

func (s singleOnlyProxy) Get(ctx context.Context, kind cache.EntryKind, hash string,
	size int64) (io.ReadCloser, int64, error) {
	return s.inner.Get(ctx, kind, hash, size)
}

func (s singleOnlyProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string,
	size int64) (bool, int64) {
	return s.inner.Contains(ctx, kind, hash, size)
}

// asProxy returns the value to hand to the disk cache: the recording proxy
// itself when batching is supported, or a narrowing wrapper otherwise.
func (p *recordingProxy) asProxy() cache.Proxy {
	if p.implementBatch {
		return p
	}
	return singleOnlyProxy{inner: p}
}

func (p *recordingProxy) Put(ctx context.Context, kind cache.EntryKind, hash string,
	logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {

	// Drain and close, like a real backend would.
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.puts = append(p.puts, fakePut{kind: kind, hash: hash, logicalSize: logicalSize})
}

func (p *recordingProxy) Get(ctx context.Context, kind cache.EntryKind, hash string,
	size int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (p *recordingProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string,
	size int64) (bool, int64) {

	if p.containsDelay != nil {
		select {
		case <-p.containsDelay:
		case <-ctx.Done():
			return false, -1
		}
	}

	p.mu.Lock()
	p.containsCalls++
	p.containsDigests++
	err := p.containsErr
	exists := err == nil && p.existing[hash]
	p.mu.Unlock()

	if err != nil {
		// The one-by-one cache.Proxy.Contains interface cannot surface
		// errors, so the disk layer treats this exactly like a miss.
		return false, -1

	}
	if exists {
		return true, size
	}
	return false, -1
}

// ContainsMany implements cache.BatchProxy. One call regardless of batch size.
func (p *recordingProxy) ContainsMany(ctx context.Context, hashes []string,
	sizes []int64) []cache.ProxyContainsResult {

	if p.containsDelay != nil {
		select {
		case <-p.containsDelay:
		case <-ctx.Done():
			results := make([]cache.ProxyContainsResult, len(hashes))
			for i := range hashes {
				results[i] = cache.ProxyContainsResult{Hash: hashes[i], Size: -1, Err: ctx.Err()}
			}
			return results
		}
	}

	p.mu.Lock()
	p.containsManyCalls++
	err := p.containsErr
	results := make([]cache.ProxyContainsResult, len(hashes))
	for i, hash := range hashes {
		results[i] = cache.ProxyContainsResult{Hash: hash, Size: -1}
		if err != nil {
			results[i].Err = err
			continue
		}
		if p.existing[hash] {
			results[i].Exists = true
			results[i].Size = sizes[i]
		}
	}
	p.mu.Unlock()

	return results
}

func (p *recordingProxy) snapshot() (puts []fakePut, containsCalls, containsManyCalls, containsDigests int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]fakePut(nil), p.puts...), p.containsCalls, p.containsManyCalls, p.containsDigests
}

func (p *recordingProxy) putCount(kind cache.EntryKind) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, put := range p.puts {
		if put.kind == kind {
			n++
		}
	}
	return n
}

// queueFullFake behaves like a proxy whose bounded upload queue is full: it
// records the drop through the shared backendproxy counter and closes the
// reader, mirroring grpcproxy.Put's default branch.
type queueFullFake struct {
	mu       sync.Mutex
	dropped  int
	puts     []fakePut
	existing map[string]bool
}

func (q *queueFullFake) Put(ctx context.Context, kind cache.EntryKind, hash string,
	logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	q.mu.Lock()
	q.dropped++
	q.mu.Unlock()
	backendproxy.RecordQueueFull(rc)
}

func (q *queueFullFake) Get(ctx context.Context, kind cache.EntryKind, hash string,
	size int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (q *queueFullFake) Contains(ctx context.Context, kind cache.EntryKind, hash string,
	size int64) (bool, int64) {
	return false, -1
}

func queueFullMetric() prometheus.Counter {
	return backendproxy.UploadRejected.WithLabelValues(string(backendproxy.QueueFull))
}

func newDiskCacheWithProxy(t *testing.T, proxy cache.Proxy, opts ...Option) *diskCache {
	t.Helper()
	cacheDir := tempDir(t)
	t.Cleanup(func() { _ = os.RemoveAll(cacheDir) })

	allOpts := append([]Option{
		WithProxyBackend(proxy),
		WithAccessLogger(testutils.NewSilentLogger()),
	}, opts...)

	c, err := New(cacheDir, 100*1024*1024, allOpts...)
	if err != nil {
		t.Fatal(err)
	}
	return c.(*diskCache)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// TestWritebackPolicyDecisions covers the policy table directly.
func TestWritebackPolicyDecisions(t *testing.T) {
	t.Parallel()

	ac := cache.AC
	cas := cache.CAS
	raw := cache.RAW

	cases := []struct {
		name       string
		mode       WritebackMode
		maxCASSize int64
		kind       cache.EntryKind
		size       int64
		want       bool
		reason     rejectReason
	}{
		{"all/ac", WritebackAll, 0, ac, 10, true, ""},
		{"all/cas", WritebackAll, 0, cas, 10, true, ""},
		{"all/raw", WritebackAll, 0, raw, 10, true, ""},
		{"all/cas-oversize", WritebackAll, 5, cas, 6, false, rejectSize},
		{"all/cas-at-limit", WritebackAll, 5, cas, 5, true, ""},
		{"cas/ac-blocked", WritebackCAS, 0, ac, 10, false, rejectKind},
		{"cas/raw-blocked", WritebackCAS, 0, raw, 10, false, rejectKind},
		{"cas/cas", WritebackCAS, 0, cas, 10, true, ""},
		{"cas/oversize", WritebackCAS, 5, cas, 6, false, rejectSize},
		{"ac/ac", WritebackAC, 0, ac, 10, true, ""},
		{"ac/raw", WritebackAC, 0, raw, 10, true, ""},
		{"ac/cas-blocked", WritebackAC, 0, cas, 10, false, rejectKind},
		{"none/ac", WritebackNone, 0, ac, 10, false, rejectKind},
		{"none/cas", WritebackNone, 0, cas, 10, false, rejectKind},
		{"none/oversize-kind-first", WritebackNone, 5, cas, 6, false, rejectKind},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := NewWritebackPolicy(tc.mode, tc.maxCASSize)
			if err != nil {
				t.Fatal(err)
			}
			got, reason := policy.shouldWriteBack(tc.kind, tc.size)
			if got != tc.want || reason != tc.reason {
				t.Fatalf("got (%v,%q), want (%v,%q)", got, reason, tc.want, tc.reason)
			}
		})
	}
}

func TestParseWritebackMode(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"all", "cas", "ac", "none"} {
		m, err := ParseWritebackMode(s)
		if err != nil || m.String() != s {
			t.Fatalf("round trip failed for %q: %v %v", s, m, err)
		}
	}
	if _, err := ParseWritebackMode("bogus"); err == nil {
		t.Fatal("expected error for bogus mode")
	}
	if _, err := NewWritebackPolicy(WritebackAll, -1); err == nil {
		t.Fatal("expected error for negative writeback size")
	}
}

// TestWritebackPolicyEndToEnd verifies, per mode, whether AC and CAS uploads
// reach the backend after a successful local Put.
func TestWritebackPolicyEndToEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	casData := []byte("some cas blob contents here")
	casHash := sha256Hex(casData)
	acData := []byte("some action result bytes")
	acHash := sha256Hex(acData)

	modes := []WritebackMode{WritebackAll, WritebackCAS, WritebackAC, WritebackNone}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			proxy := newRecordingProxy(true)
			policy, err := NewWritebackPolicy(mode, 0)
			if err != nil {
				t.Fatal(err)
			}
			c := newDiskCacheWithProxy(t, proxy, WithWritebackPolicy(policy))

			if err := c.Put(ctx, cache.CAS, casHash, int64(len(casData)),
				bytes.NewReader(casData)); err != nil {
				t.Fatal(err)
			}
			if err := c.Put(ctx, cache.AC, acHash, int64(len(acData)),
				bytes.NewReader(acData)); err != nil {
				t.Fatal(err)
			}

			wantCAS := mode == WritebackAll || mode == WritebackCAS
			wantAC := mode == WritebackAll || mode == WritebackAC

			if got := proxy.putCount(cache.CAS); got > 0 != wantCAS {
				t.Fatalf("CAS writeback: got %d puts, want present=%v", got, wantCAS)
			}
			if got := proxy.putCount(cache.AC); got > 0 != wantAC {
				t.Fatalf("AC writeback: got %d puts, want present=%v", got, wantAC)
			}

			// Local cache behavior is unchanged: both entries are readable.
			if ok, _ := c.Contains(ctx, cache.CAS, casHash, int64(len(casData))); !ok {
				t.Fatal("local CAS entry missing despite policy")
			}
			if ok, _ := c.Contains(ctx, cache.AC, acHash, int64(len(acData))); !ok {
				t.Fatal("local AC entry missing despite policy")
			}
		})
	}
}

// TestWritebackCASSizeLimit verifies the push-side size limit and the
// rejection metric, independently of the pull-side maxProxyBlobSize.
func TestWritebackCASSizeLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	proxy := newRecordingProxy(true)
	policy, err := NewWritebackPolicy(WritebackAll, 10)
	if err != nil {
		t.Fatal(err)
	}
	c := newDiskCacheWithProxy(t, proxy, WithWritebackPolicy(policy))

	small := []byte("small")
	smallHash := sha256Hex(small)
	big := bytes.Repeat([]byte("x"), 100)
	bigHash := sha256Hex(big)

	before := testutil.ToFloat64(writebackRejected.WithLabelValues(string(rejectSize)))

	if err := c.Put(ctx, cache.CAS, smallHash, int64(len(small)), bytes.NewReader(small)); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, cache.CAS, bigHash, int64(len(big)), bytes.NewReader(big)); err != nil {
		t.Fatal(err)
	}

	if got := proxy.putCount(cache.CAS); got != 1 {
		t.Fatalf("expected exactly 1 CAS upload, got %d", got)
	}
	after := testutil.ToFloat64(writebackRejected.WithLabelValues(string(rejectSize)))
	if after-before != 1 {
		t.Fatalf("expected 1 size rejection, got %v", after-before)
	}

	// The oversize entry is still committed locally.
	if ok, _ := c.Contains(ctx, cache.CAS, bigHash, int64(len(big))); !ok {
		t.Fatal("oversize entry should still be present in the local cache")
	}
}

// TestWritebackKindRejectionMetric verifies the kind-disabled counter.
func TestWritebackKindRejectionMetric(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	proxy := newRecordingProxy(true)
	policy, _ := NewWritebackPolicy(WritebackNone, 0)
	c := newDiskCacheWithProxy(t, proxy, WithWritebackPolicy(policy))

	data := []byte("hello world")
	hash := sha256Hex(data)

	before := testutil.ToFloat64(writebackRejected.WithLabelValues(string(rejectKind)))
	if err := c.Put(ctx, cache.CAS, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if got := proxy.putCount(cache.CAS); got != 0 {
		t.Fatalf("expected no uploads under writeback=none, got %d", got)
	}
	after := testutil.ToFloat64(writebackRejected.WithLabelValues(string(rejectKind)))
	if after-before != 1 {
		t.Fatalf("expected 1 kind rejection, got %v", after-before)
	}
}

// TestQueueFullCounter uses a proxy whose Put always drops the upload
// (simulating a full queue) and checks the queue-full metric is incremented.
func TestQueueFullCounter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	queueFullProxy := &queueFullFake{existing: map[string]bool{}}
	c := newDiskCacheWithProxy(t, queueFullProxy)

	data, hash := testutils.RandomDataAndHash(50)
	before := testutil.ToFloat64(queueFullMetric())
	if err := c.Put(ctx, cache.CAS, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	after := testutil.ToFloat64(queueFullMetric())
	if after-before != 1 {
		t.Fatalf("expected one queue-full rejection, got %v", after-before)
	}
}

// TestWritebackACDanglingWarning verifies that the AC-only mode emits a
// startup warning about dangling entries, while the other modes stay quiet.
func TestWritebackACDanglingWarning(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	warningLogger := log.New(&buf, "", 0)

	policy, err := NewWritebackPolicy(WritebackAC, 0)
	if err != nil {
		t.Fatal(err)
	}
	policy.warnAboutDanglingAC(warningLogger)
	if !strings.Contains(buf.String(), "action cache entries") {
		t.Fatalf("expected a dangling-AC warning, got %q", buf.String())
	}

	for _, mode := range []WritebackMode{WritebackAll, WritebackCAS, WritebackNone} {
		var b bytes.Buffer
		p, err := NewWritebackPolicy(mode, 0)
		if err != nil {
			t.Fatal(err)
		}
		p.warnAboutDanglingAC(log.New(&b, "", 0))
		if b.Len() != 0 {
			t.Fatalf("mode %s should not warn, got %q", mode, b.String())
		}
	}
}
