package grpcproxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk"
	"github.com/buchgr/bazel-remote/v2/server"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

var logger = testutils.NewSilentLogger()

type testProxy struct {
	pb.UnimplementedContentAddressableStorageServer
	dir    string
	server *grpc.Server
	proxy  cache.Proxy

	// Test instrumentation for FindMissingBlobs. The mutex guards the
	// counters because batched lookups issue concurrent RPCs.
	metricsMu        sync.Mutex
	findMissingCalls int
	findMissingBatch []int // number of digests in each request
	totalFindMissing int
	findMissingErr   error
	emptyGetReader   bool // when true, GetActionResult returns nil reader semantics
}

func (p *testProxy) Contains(kind cache.EntryKind, hash string) bool {
	src := filepath.Join(p.dir, kind.DirName(), hash)
	if _, err := os.Stat(src); err != nil {
		return false
	}
	return true
}

func newProxy(t *testing.T, dir string, storageMode string) *testProxy {
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	p := &testProxy{
		dir:    dir,
		server: srv,
	}
	pb.RegisterActionCacheServer(srv, p)
	pb.RegisterCapabilitiesServer(srv, p)
	pb.RegisterContentAddressableStorageServer(srv, p)
	bs.RegisterByteStreamServer(srv, p)

	go func() {
		_ = srv.Serve(listener)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	cc, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		t.Fatal(err)
	}
	clients := NewGrpcClients(cc)
	err = clients.CheckCapabilities(storageMode == "zstd")
	if err != nil {
		t.Fatal(err)
	}
	p.proxy = New(clients, storageMode, logger, logger, 100, 100)

	return p
}

func (p *testProxy) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions:               []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
			SupportedCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
		},
	}, nil
}

func (p *testProxy) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	data, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return nil, err
	}
	dest := filepath.Join(p.dir, cache.AC.DirName(), req.ActionDigest.Hash)
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(dest)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return req.ActionResult, nil
}

func (p *testProxy) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	if p.emptyGetReader {
		// A non-NotFound error makes grpcproxy.Get return a nil reader
		// together with an error. The old grpcproxy.Contains called
		// rc.Close() before checking the error and panicked on the nil
		// reader.
		return nil, status.Error(codes.Internal, "synthetic empty-reader failure")
	}
	src := filepath.Join(p.dir, cache.AC.DirName(), req.ActionDigest.Hash)
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	result := &pb.ActionResult{}
	if proto.Unmarshal(data, result) != nil {
		return nil, err
	}
	return result, nil
}

func (p *testProxy) FindMissingBlobs(tx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	p.metricsMu.Lock()
	p.findMissingCalls++
	p.findMissingBatch = append(p.findMissingBatch, len(req.BlobDigests))
	p.totalFindMissing += len(req.BlobDigests)
	findMissingErr := p.findMissingErr
	p.metricsMu.Unlock()

	if findMissingErr != nil {
		return nil, findMissingErr
	}

	digests := []*pb.Digest{}
	for _, digest := range req.BlobDigests {
		src := filepath.Join(p.dir, cache.CAS.DirName(), digest.Hash)
		if _, err := os.Stat(src); err != nil {
			digests = append(digests, digest)
		}
	}
	result := pb.FindMissingBlobsResponse{
		MissingBlobDigests: digests,
	}
	return &result, nil
}

func (p *testProxy) Read(req *bs.ReadRequest, resp bs.ByteStream_ReadServer) error {
	parts := strings.Split(req.ResourceName, "/")
	hash := parts[len(parts)-2]
	f, err := os.Open(filepath.Join(p.dir, cache.CAS.DirName(), hash))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	err = resp.Send(&bs.ReadResponse{Data: data[:len(data)/2]})
	if err != nil {
		return err
	}
	return resp.Send(&bs.ReadResponse{Data: data[len(data)/2:]})
}

func (p *testProxy) Write(srv bs.ByteStream_WriteServer) error {
	var f *os.File
	for {
		req, err := srv.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if f == nil {
			parts := strings.Split(req.ResourceName, "/")
			hash := parts[len(parts)-2]
			dest := filepath.Join(p.dir, cache.CAS.DirName(), hash)
			if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
				return err
			}
			f, err = os.Create(dest)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
		}
		_, err = f.Write(req.Data)
		if err != nil {
			return err
		}
	}
}

func (p *testProxy) QueryWriteStatus(context.Context, *bs.QueryWriteStatusRequest) (*bs.QueryWriteStatusResponse, error) {
	return nil, nil
}

type fixture struct {
	cc      *grpc.ClientConn
	server  *grpc.Server
	clients *GrpcClients
	cache   disk.Cache
}

func newFixture(t *testing.T, proxy cache.Proxy, storageMode string) *fixture {
	listener := bufconn.Listen(1024 * 1024)
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}

	cc, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		t.Fatal(err)
	}
	diskCache, err := disk.New(
		testutils.TempDir(t),
		8*1024*1024,
		disk.WithProxyBackend(proxy),
		disk.WithStorageMode(storageMode),
		disk.WithAccessLogger(logger),
		disk.WithMaxBlobSize(math.MaxInt64))
	if err != nil {
		t.Fatal(err)
	}

	unlimitedMaxCasBlobSize := int64(math.MaxInt64)

	grpcServer := grpc.NewServer()

	go func() {
		err := server.ServeGRPC(listener, grpcServer, false, false, true, unlimitedMaxCasBlobSize, diskCache, logger, logger)
		if err != nil {
			logger.Printf("%s", err.Error())
		}
	}()

	clients := NewGrpcClients(cc)

	return &fixture{
		cache:   diskCache,
		cc:      cc,
		server:  grpcServer,
		clients: clients,
	}
}

func runTest(t *testing.T, storageMode string) {
	proxyFixture := newProxy(t, testutils.TempDir(t), storageMode)
	putFixture := newFixture(t, proxyFixture.proxy, storageMode)
	getFixture := newFixture(t, proxyFixture.proxy, storageMode)
	time.Sleep(time.Second)

	data, digest := testutils.RandomDataAndDigest(3 * 1024 * 1024)

	putCasReq := &bs.WriteRequest{
		ResourceName: fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid.New().String(), digest.Hash, digest.SizeBytes),
		Data:         data,
	}
	putClient, err := putFixture.clients.bs.Write(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = putClient.Send(putCasReq)
	if err != nil {
		t.Fatal(err)
	}
	_, err = putClient.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	ar := pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{
			Path:   "foo/bar",
			Digest: &digest,
		}},
		ExitCode: int32(42),
		ExecutionMetadata: &pb.ExecutedActionMetadata{
			Worker: "Test",
		},
	}
	arData, err := proto.Marshal(&ar)
	if err != nil {
		t.Fatal(err)
	}
	arSum := sha256.Sum256(arData)
	arDigest := pb.Digest{
		Hash:      hex.EncodeToString(arSum[:]),
		SizeBytes: int64(len(arData)),
	}
	putAcReq := pb.UpdateActionResultRequest{
		ActionDigest: &arDigest,
		ActionResult: &ar,
	}
	_, err = putFixture.clients.ac.UpdateActionResult(context.Background(), &putAcReq)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	ok, size := putFixture.cache.Contains(context.Background(), cache.AC, arDigest.Hash, arDigest.SizeBytes)
	if !ok || size != arDigest.SizeBytes {
		t.Fatal("Cound not find action result in first server")
	}
	ok, size = putFixture.cache.Contains(context.Background(), cache.CAS, digest.Hash, digest.SizeBytes)
	if !ok || size != digest.SizeBytes {
		t.Fatal("Cound not find blob in first server")
	}
	if !proxyFixture.Contains(cache.AC, arDigest.Hash) {
		t.Fatal("Cound not find action result in proxy")
	}
	if !proxyFixture.Contains(cache.CAS, digest.Hash) {
		t.Fatal("Cound not find blob in proxy")
	}

	ok, size = getFixture.cache.Contains(context.Background(), cache.AC, arDigest.Hash, arDigest.SizeBytes)
	if !ok || size != arDigest.SizeBytes {
		t.Fatal("Second server could not find action result")
	}
	ok, size = getFixture.cache.Contains(context.Background(), cache.CAS, digest.Hash, digest.SizeBytes)
	if !ok || size != digest.SizeBytes {
		t.Fatal("Second server could not find blob")
	}

	fmReq := pb.FindMissingBlobsRequest{
		BlobDigests: []*pb.Digest{&digest},
	}
	fmRes, err := getFixture.clients.cas.FindMissingBlobs(context.Background(), &fmReq)
	if err != nil {
		t.Fatal(err)
	}
	if len(fmRes.MissingBlobDigests) > 0 {
		t.Fatal("Second server did not find blob on proxy")
	}

	getAcReq := pb.GetActionResultRequest{
		ActionDigest: &arDigest,
	}
	getAcRes, err := getFixture.clients.ac.GetActionResult(context.Background(), &getAcReq)
	if err != nil {
		t.Fatal(err)
	}
	if err == nil && len(getAcRes.OutputFiles) < 1 {
		t.Fatal("Missing output files from action result")
	}
	if err == nil && getAcRes.OutputFiles[0].Digest.Hash != digest.Hash {
		t.Fatal("Unexpected digest in action result")
	}

	getCasRequest := &bs.ReadRequest{
		ResourceName: fmt.Sprintf("blobs/%s/%d", digest.Hash, digest.SizeBytes),
	}
	getClient, err := getFixture.clients.bs.Read(context.Background(), getCasRequest)
	if err != nil {
		t.Fatal(err)
	}
	received := []byte{}
	for {
		res, err := getClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, res.Data...)
	}

	if len(received) != len(data) {
		t.Fatal("Unexpected blob size")
	}
}

func TestEverything(t *testing.T) {
	runTest(t, "uncompressed")
}

func TestEverythingZstd(t *testing.T) {
	runTest(t, "zstd")
}

// TestContainsManySingleRPC verifies that a batched existence check against
// the gRPC backend results in exactly one FindMissingBlobs call carrying all
// digests, regardless of how the disk cache splits its work.
func TestContainsManySingleRPC(t *testing.T) {
	t.Parallel()

	proxyFixture := newProxy(t, testutils.TempDir(t), "uncompressed")
	t.Cleanup(proxyFixture.server.Stop)

	// Seed some blobs in the backend so the hit/miss mix is meaningful.
	var existingHashes []string
	for i := 0; i < 3; i++ {
		data, digest := testutils.RandomDataAndDigest(100)
		dest := filepath.Join(proxyFixture.dir, cache.CAS.DirName(), digest.Hash)
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dest, data, 0644); err != nil {
			t.Fatal(err)
		}
		existingHashes = append(existingHashes, digest.Hash)
	}

	// Build a batch mixing existing and non-existing digests.
	hashes := make([]string, 0, 8)
	sizes := make([]int64, 0, 8)
	for _, h := range existingHashes {
		hashes = append(hashes, h)
		sizes = append(sizes, 100)
	}
	for i := 0; i < 5; i++ {
		_, d := testutils.RandomDataAndDigest(200)
		hashes = append(hashes, d.Hash)
		sizes = append(sizes, d.SizeBytes)
	}

	batchProxy := proxyFixture.proxy.(cache.BatchProxy)
	results := batchProxy.ContainsMany(context.Background(), hashes, sizes)

	if len(results) != len(hashes) {
		t.Fatalf("expected %d results, got %d", len(hashes), len(results))
	}
	if proxyFixture.findMissingCalls != 1 {
		t.Fatalf("expected exactly 1 FindMissingBlobs RPC, got %d", proxyFixture.findMissingCalls)
	}
	if proxyFixture.totalFindMissing != len(hashes) {
		t.Fatalf("expected all %d digests in the single RPC, got %d",
			len(hashes), proxyFixture.totalFindMissing)
	}

	existingSet := map[string]bool{}
	for _, h := range existingHashes {
		existingSet[h] = true
	}
	for i, res := range results {
		if res.Hash != hashes[i] {
			t.Fatalf("result ordering mismatch at %d", i)
		}
		wantExists := existingSet[hashes[i]]
		if res.Exists != wantExists {
			t.Fatalf("digest %d: expected exists=%v, got %v", i, wantExists, res.Exists)
		}
		if res.Err != nil {
			t.Fatalf("digest %d: unexpected error %v", i, res.Err)
		}
	}
}

// TestContainsManyBackendError verifies that an RPC failure fails every
// result (with the error attached), so the disk layer reports misses rather
// than false hits.
func TestContainsManyBackendError(t *testing.T) {
	t.Parallel()

	proxyFixture := newProxy(t, testutils.TempDir(t), "uncompressed")
	t.Cleanup(proxyFixture.server.Stop)
	proxyFixture.findMissingErr = status.Error(codes.Unavailable, "backend down")

	_, d := testutils.RandomDataAndDigest(100)
	batchProxy := proxyFixture.proxy.(cache.BatchProxy)
	results := batchProxy.ContainsMany(context.Background(),
		[]string{d.Hash}, []int64{d.SizeBytes})

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Exists {
		t.Fatal("errored backend must not report existence")
	}
	if results[0].Err == nil {
		t.Fatal("expected the RPC error to be attached to the result")
	}
}

// TestContainsACNilReader verifies that the AC existence path no longer
// panics when the backend returns a nil reader/empty result. Previously
// grpcproxy.Contains called Close on a nil io.ReadCloser.
func TestContainsACNilReader(t *testing.T) {
	t.Parallel()

	proxyFixture := newProxy(t, testutils.TempDir(t), "uncompressed")
	t.Cleanup(proxyFixture.server.Stop)
	proxyFixture.emptyGetReader = true

	_, digest := testutils.RandomDataAndDigest(64)

	// This must return cleanly instead of panicking on a nil reader.
	ok, size := proxyFixture.proxy.Contains(context.Background(),
		cache.AC, digest.Hash, digest.SizeBytes)
	if ok || size != -1 {
		t.Fatalf("expected a miss for an empty AC result, got ok=%v size=%d", ok, size)
	}
}

// TestDiskFindMissingBatchesOverGrpc drives the full path: a disk cache
// backed by the gRPC proxy. Repeated digests are checked once and the
// configured batch size controls how many FindMissingBlobs RPCs the fake
// backend receives.
func TestDiskFindMissingBatchesOverGrpc(t *testing.T) {
	t.Parallel()

	proxyFixture := newProxy(t, testutils.TempDir(t), "uncompressed")
	t.Cleanup(proxyFixture.server.Stop)

	diskCache, err := disk.New(
		testutils.TempDir(t),
		8*1024*1024,
		disk.WithProxyBackend(proxyFixture.proxy),
		disk.WithStorageMode("uncompressed"),
		disk.WithAccessLogger(logger),
		disk.WithProxyContainsBatchSize(3),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 4 unique digests, with duplicates, all absent locally and on the
	// backend.
	_, d1 := testutils.RandomDataAndDigest(100)
	_, d2 := testutils.RandomDataAndDigest(100)
	_, d3 := testutils.RandomDataAndDigest(100)
	_, d4 := testutils.RandomDataAndDigest(100)

	blobs := []*pb.Digest{&d1, &d1, &d2, &d3, &d3, &d3, &d4}

	missing, err := diskCache.FindMissingCasBlobs(context.Background(), blobs)
	if err != nil {
		t.Fatal(err)
	}
	// All digests are missing, so every input slot survives, but the set of
	// distinct hashes must still be exactly the four unique ones.
	uniqueMissing := map[string]bool{}
	for _, d := range missing {
		uniqueMissing[d.Hash] = true
	}
	if len(uniqueMissing) != 4 {
		t.Fatalf("expected 4 unique missing digests, got %d", len(uniqueMissing))
	}

	// Four unique digests with a batch size of three => two RPCs, and the
	// duplicate entries must not add any extra digests. Batches run
	// concurrently, so their completion (and thus RPC) order is not
	// guaranteed; assert the multiset of batch sizes instead.
	if proxyFixture.findMissingCalls != 2 {
		t.Fatalf("expected 2 batched FindMissingBlobs RPCs, got %d",
			proxyFixture.findMissingCalls)
	}
	if proxyFixture.totalFindMissing != 4 {
		t.Fatalf("expected 4 unique digests sent to the backend, got %d",
			proxyFixture.totalFindMissing)
	}
	batches := append([]int(nil), proxyFixture.findMissingBatch...)
	sort.Ints(batches)
	if len(batches) != 2 || batches[0] != 1 || batches[1] != 3 {
		t.Fatalf("expected batch sizes {1,3}, got %v", proxyFixture.findMissingBatch)
	}
}
