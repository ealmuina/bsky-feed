// Package heapwatch is a defense-in-depth safety net: when the Go heap
// grows past a configured threshold, it writes a pprof heap profile to
// disk so the next leak can be diagnosed even if the OOM killer fires
// before anyone notices.
//
// Run blocks for the lifetime of the process. It is intentionally
// panic-free (only stdlib calls and log output) and should not be
// wrapped in utils.Recoverer.
package heapwatch

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// defaultThresholdBytes is the HeapAlloc threshold that triggers a
	// dump. 4 GiB matches roughly 70% of the 6 GiB container memory cap
	// set in docker-compose.yml — enough headroom to capture a profile
	// before the OOM kill.
	defaultThresholdBytes uint64 = 4 << 30

	// hysteresisFraction: after firing, the watcher re-arms only when
	// HeapAlloc drops below this fraction of the threshold. Prevents
	// disk-fill when the process sits near the limit for a long time.
	hysteresisFraction float64 = 0.75

	// sampleInterval — runtime.ReadMemStats is cheap; 1 Hz is plenty for
	// detecting a multi-GB leak that takes hours to build.
	sampleInterval = 1 * time.Second

	// dirName is the relative sub-directory under the process CWD where
	// dumps are written. Matches the workdir the api container runs in.
	dirName = "heapdumps"
)

// envThresholdName is the env var operators can use to override the
// default threshold. Useful when running with a different container
// memory cap or in dev.
const envThresholdName = "HEAPWATCH_THRESHOLD_BYTES"

// Run blocks for the lifetime of the process. Call it as `go heapwatch.Run()`
// from main; do not wrap in Recoverer.
func Run() {
	threshold := defaultThresholdBytes
	if v := os.Getenv(envThresholdName); v != "" {
		var parsed uint64
		if _, err := fmt.Sscanf(v, "%d", &parsed); err == nil && parsed > 0 {
			threshold = parsed
		} else {
			log.Warnf("heapwatch: invalid %s=%q, using default %d bytes",
				envThresholdName, v, defaultThresholdBytes)
		}
	}

	if err := os.MkdirAll(dirName, 0o755); err != nil {
		log.Errorf("heapwatch: cannot create %s: %v; watcher disabled", dirName, err)
		return
	}

	log.Warnf("heapwatch: armed, threshold=%d bytes (%.2f GiB), dir=%s",
		threshold, float64(threshold)/(1<<30), dirName)

	var memStats runtime.MemStats
	armed := true

	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	for range ticker.C {
		runtime.ReadMemStats(&memStats)
		switch {
		case armed && memStats.HeapAlloc >= threshold:
			armed = false
			path := writeDump()
			if path != "" {
				log.Errorf("heapwatch: HEAP DUMP WRITTEN to %s "+
					"(HeapAlloc=%d bytes, ~%.2f GiB). "+
					"Analyze with: go tool pprof %s",
					path, memStats.HeapAlloc,
					float64(memStats.HeapAlloc)/(1<<30), path)
			}
		case !armed && memStats.HeapAlloc < uint64(float64(threshold)*hysteresisFraction):
			armed = true
			log.Warnf("heapwatch: re-armed (HeapAlloc=%d bytes)", memStats.HeapAlloc)
		}
	}
}

// writeDump creates a heap-<unix>.pb.gz in dirName and writes the heap
// profile into it. Returns the path on success, "" on failure.
func writeDump() string {
	path := filepath.Join(dirName, fmt.Sprintf("heap-%d.pb.gz", time.Now().Unix()))
	f, err := os.Create(path)
	if err != nil {
		log.Errorf("heapwatch: cannot create %s: %v", path, err)
		return ""
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Errorf("heapwatch: WriteHeapProfile failed: %v", err)
		_ = f.Close()
		_ = os.Remove(path)
		return ""
	}
	if err := f.Close(); err != nil {
		log.Errorf("heapwatch: close failed: %v", err)
	}
	return path
}
