package integration

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestServicesReachable(t *testing.T) {
	waitForTCP(t, "mongodb", "localhost:27017", 30*time.Second)
	waitForTCP(t, "postgresql", "localhost:5432", 30*time.Second)
}

func waitForTCP(t *testing.T, name, address string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("%s not reachable at %s within %s", name, address, timeout)
}

func TestConnectorBuildSkipped(t *testing.T) {
	t.Skip("full connector integration requires Couchbase checkpoint metadata")
	_ = context.Background()
}
