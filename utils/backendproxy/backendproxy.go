package backendproxy

import (
	"io"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type UploadReq struct {
	Hash        string
	LogicalSize int64
	SizeOnDisk  int64
	Kind        cache.EntryKind
	Rc          io.ReadCloser
}

type Uploader interface {
	UploadFile(item UploadReq)
}

// RejectReason labels the counters below.
type RejectReason string

const (
	// QueueFull means the bounded upload queue had no capacity and the
	// upload was dropped.
	QueueFull RejectReason = "queue_full"

	// BackendError means the upload was attempted but the proxy backend
	// returned an error.
	BackendError RejectReason = "backend_error"
)

// UploadRejected counts uploads that did not reach the backend successfully.
// Policy-driven rejections (disabled entry kind, oversize CAS) are counted
// separately by the disk cache, before an upload is ever enqueued.
var UploadRejected = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_proxy_upload_rejected_total",
	Help: "The number of writeback uploads that did not complete on the proxy backend. The " +
		"reason label is \"queue_full\" when the bounded upload queue was full, or " +
		"\"backend_error\" when the backend rejected or failed the upload.",
}, []string{"reason"})

// RecordQueueFull increments the queue-full counter and closes the reader
// that would have been uploaded.
func RecordQueueFull(rc io.ReadCloser) {
	UploadRejected.WithLabelValues(string(QueueFull)).Inc()
	if rc != nil {
		_ = rc.Close()
	}
}

// RecordBackendError increments the backend-error counter.
func RecordBackendError() {
	UploadRejected.WithLabelValues(string(BackendError)).Inc()
}

func StartUploaders(u Uploader, numUploaders int, maxQueuedUploads int) chan UploadReq {
	if maxQueuedUploads <= 0 || numUploaders <= 0 {
		return nil
	}

	uploadQueue := make(chan UploadReq, maxQueuedUploads)

	for i := 0; i < numUploaders; i++ {
		go func() {
			for item := range uploadQueue {
				u.UploadFile(item)
			}
		}()
	}

	return uploadQueue
}
