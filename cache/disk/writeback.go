package disk

import (
	"fmt"
	"log"
	"math"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
)

// WritebackMode selects which kinds of locally committed cache entries are
// also written back to the proxy backend.
type WritebackMode int

const (
	// WritebackAll writes both AC (and RAW) and CAS entries back to the
	// proxy backend. This is the historical behavior and remains the
	// default.
	WritebackAll WritebackMode = iota

	// WritebackCAS only writes CAS entries back.
	WritebackCAS

	// WritebackAC only writes AC (and RAW) entries back. Note that this
	// can create dangling action cache entries on the backend when their
	// CAS dependencies were never uploaded; a startup warning is emitted.
	WritebackAC

	// WritebackNone never writes anything back to the proxy backend,
	// suitable for read-only backends.
	WritebackNone
)

// String returns the stable, user-facing name of the writeback mode, as
// used in YAML configuration and on the command line.
func (m WritebackMode) String() string {
	switch m {
	case WritebackAll:
		return "all"
	case WritebackCAS:
		return "cas"
	case WritebackAC:
		return "ac"
	case WritebackNone:
		return "none"
	default:
		return "unknown"
	}
}

// ParseWritebackMode parses a writeback mode name as accepted by the
// configuration.
func ParseWritebackMode(s string) (WritebackMode, error) {
	switch s {
	case "all":
		return WritebackAll, nil
	case "cas":
		return WritebackCAS, nil
	case "ac":
		return WritebackAC, nil
	case "none":
		return WritebackNone, nil
	default:
		return WritebackAll, fmt.Errorf(
			"invalid writeback mode %q: must be one of \"all\", \"cas\", \"ac\" or \"none\"", s)
	}
}

// WritebackPolicy decides whether a cache entry that has just been
// committed locally should also be uploaded to the proxy backend.
type WritebackPolicy struct {
	// mode selects which entry kinds are written back.
	mode WritebackMode

	// maxCASBlobSize is the maximum logical size of a CAS entry that is
	// written back. A non-positive value means that CAS entries are not
	// size-limited. This is deliberately independent of
	// maxProxyBlobSize, which limits what is downloaded from the backend:
	// push and pull limits answer different questions.
	maxCASBlobSize int64
}

// DefaultWritebackPolicy returns the policy that reproduces the historical
// behavior: every entry is written back, regardless of kind or size.
func DefaultWritebackPolicy() WritebackPolicy {
	return WritebackPolicy{
		mode:           WritebackAll,
		maxCASBlobSize: math.MaxInt64,
	}
}

// NewWritebackPolicy validates and constructs a writeback policy. A
// non-positive maxCASBlobSize disables the CAS size limit.
func NewWritebackPolicy(mode WritebackMode, maxCASBlobSize int64) (WritebackPolicy, error) {
	if maxCASBlobSize < 0 {
		return WritebackPolicy{}, fmt.Errorf(
			"invalid max writeback CAS blob size: %d, must be >= 0", maxCASBlobSize)
	}
	if maxCASBlobSize == 0 {
		maxCASBlobSize = math.MaxInt64
	}
	return WritebackPolicy{
		mode:           mode,
		maxCASBlobSize: maxCASBlobSize,
	}, nil
}

// rejectReason describes why a writeback was skipped. The empty string
// means that the entry should be written back.
type rejectReason string

const (
	rejectKind rejectReason = "kind_disabled"
	rejectSize rejectReason = "too_large"
)

// shouldWriteBack reports whether an entry of the given kind and logical
// size should be uploaded to the proxy backend, and why not.
func (p WritebackPolicy) shouldWriteBack(kind cache.EntryKind, logicalSize int64) (bool, rejectReason) {
	switch kind {
	case cache.CAS:
		if p.mode != WritebackAll && p.mode != WritebackCAS {
			return false, rejectKind
		}
		if logicalSize > p.maxCASBlobSize {
			return false, rejectSize
		}
	case cache.AC, cache.RAW:
		if p.mode != WritebackAll && p.mode != WritebackAC {
			return false, rejectKind
		}
	}
	return true, ""
}

// warnAboutDanglingAC logs a startup warning for mode combinations that can
// leave dangling AC entries on the backend.
func (p WritebackPolicy) warnAboutDanglingAC(logger *log.Logger) {
	if p.mode != WritebackAC {
		return
	}
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("WARNING: writeback mode %q uploads action cache entries but not their CAS "+
		"dependencies. Other instances may hit these entries and still have to fetch the missing "+
		"CAS blobs; consider using writeback mode %q instead.", WritebackAC, WritebackAll)
}

// writebackRejected counts cache entries that were not handed to the proxy
// backend because the configured writeback policy rejected them. It is
// registered on the default Prometheus registry when endpoint metrics are
// enabled.
var writebackRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_proxy_writeback_rejected_total",
	Help: "The number of cache entries that were not written back to the proxy backend. " +
		"The reason label is either \"kind_disabled\" (the entry kind is excluded by the " +
		"writeback mode) or \"too_large\" (a CAS entry exceeded the writeback size limit).",
}, []string{"reason"})
