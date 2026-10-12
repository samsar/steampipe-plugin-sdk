package plugin_test

// Regression tests for two bugs in how a scan reports a failed hydrate call.
//
//   - Lost error. streamRows waits in one select on errorChan and rowChan. When
//     the last row to complete fails, its error is sent on errorChan and then
//     rowChan is closed. If streamRows is busy sending a row while that happens,
//     both channels are ready at its next select and Go picks one at random:
//     about half the time the scan is reported as a success with the failed row
//     missing.
//
//   - Leaked goroutines. errorChan holds one error, and nothing reads it after
//     streamRows has returned. When more rows fail after that, each sender
//     blocks forever.
//
// Run:  go test -race -run 'TestExecute.*Fail' ./plugin/

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"

	sdkgrpc "github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin/transform"
)

const (
	scanErrorConnection = "scan_error_fixture"
	lastRowFailsTable   = "last_row_fails"
	allRowsFailTable    = "all_rows_fail"
	// the stream holds the first row, the output channel buffers the second,
	// and streamRows blocks sending the third while the fourth fails
	lastRowFailsRows = 4
	allRowsFailRows  = 10
)

type scanErrorItem struct {
	ID int
}

// failGateKey is the context key of a channel which the failing hydrate call
// waits on before it returns its error.
type failGateKey struct{}

func listLastRowFailsItems(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	for i := 0; i < lastRowFailsRows; i++ {
		d.StreamListItem(ctx, scanErrorItem{ID: i})
	}
	return nil, nil
}

func listAllRowsFailItems(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	for i := 0; i < allRowsFailRows; i++ {
		d.StreamListItem(ctx, scanErrorItem{ID: i})
	}
	return nil, nil
}

// lastRowFailsDetail succeeds for every row except the last one, which fails
// (after the gate in the context is closed, if there is one).
func lastRowFailsDetail(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	if h.Item.(scanErrorItem).ID < lastRowFailsRows-1 {
		return "ok", nil
	}
	if gate, ok := ctx.Value(failGateKey{}).(chan struct{}); ok {
		<-gate
	}
	return nil, errors.New("hydrate failed on the last row")
}

func allRowsFailDetail(context.Context, *plugin.QueryData, *plugin.HydrateData) (any, error) {
	return nil, errors.New("hydrate failed on every row")
}

func scanErrorFixturePlugin(_ context.Context) *plugin.Plugin {
	columns := func(detail plugin.HydrateFunc) []*plugin.Column {
		return []*plugin.Column{
			{Name: "id", Type: proto.ColumnType_INT, Transform: transform.FromField("ID")},
			{Name: "detail", Type: proto.ColumnType_STRING, Hydrate: detail, Transform: transform.FromValue()},
		}
	}
	return &plugin.Plugin{
		Name: "steampipe-plugin-scan-error-fixture",
		TableMap: map[string]*plugin.Table{
			lastRowFailsTable: {
				Name:    lastRowFailsTable,
				List:    &plugin.ListConfig{Hydrate: listLastRowFailsItems},
				Columns: columns(lastRowFailsDetail),
			},
			allRowsFailTable: {
				Name:    allRowsFailTable,
				List:    &plugin.ListConfig{Hydrate: listAllRowsFailItems},
				Columns: columns(allRowsFailDetail),
			},
		},
	}
}

func newScanErrorServer(t *testing.T) *sdkgrpc.PluginServer {
	t.Helper()
	server := plugin.Server(&plugin.ServeOpts{PluginFunc: scanErrorFixturePlugin})
	if _, err := server.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false, MaxSizeMb: 32}); err != nil {
		t.Fatalf("SetCacheOptions failed: %v", err)
	}
	cfg := &proto.ConnectionConfig{
		Connection:      scanErrorConnection,
		Plugin:          "steampipe-plugin-scan-error-fixture",
		PluginShortName: "scan-error-fixture",
		PluginInstance:  "steampipe-plugin-scan-error-fixture",
	}
	if _, err := server.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{cfg},
		MaxCacheSizeMb: 32,
	}); err != nil {
		t.Fatalf("SetAllConnectionConfigs: %v", err)
	}
	return server
}

// executeStream is an in-memory Execute stream. If firstRow is set, the first
// Send closes it and then waits for release, so the rows behind it back up
// inside the plugin.
type executeStream struct {
	grpc.ServerStream
	ctx      context.Context
	firstRow chan struct{}
	release  chan struct{}
}

func (s *executeStream) Context() context.Context { return s.ctx }

func (s *executeStream) Send(*proto.ExecuteResponse) error {
	if s.firstRow != nil {
		close(s.firstRow)
		s.firstRow = nil
		<-s.release
	}
	return nil
}

func executeScan(server *sdkgrpc.PluginServer, table string, stream *executeStream) error {
	req := &proto.ExecuteRequest{
		Table:        table,
		QueryContext: proto.NewQueryContext([]string{"id", "detail"}, nil, -1, nil),
		CallId:       sdkgrpc.BuildCallId(),
		Connection:   scanErrorConnection,
		ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{
			scanErrorConnection: {CacheEnabled: false},
		},
	}
	return server.Execute(req, stream)
}

// TestExecuteReturnsErrorWhenLastRowFails holds the first row in the stream, so
// streamRows is blocked sending a row while the last row fails, its error is
// queued and the row channel is closed. Both channels are then ready at the
// next select. Without the fix about half of these scans return no error.
func TestExecuteReturnsErrorWhenLastRowFails(t *testing.T) {
	const (
		scans  = 20
		settle = 25 * time.Millisecond
	)
	server := newScanErrorServer(t)

	for i := 0; i < scans; i++ {
		failNow := make(chan struct{})
		firstRow := make(chan struct{})
		release := make(chan struct{})
		stream := &executeStream{
			ctx:      context.WithValue(context.Background(), failGateKey{}, failNow),
			firstRow: firstRow,
			release:  release,
		}
		go func() {
			<-firstRow
			// let the other successful rows back up behind the held row
			time.Sleep(settle)
			// fail the last row, then let its error be queued and the row channel close
			close(failNow)
			time.Sleep(settle)
			close(release)
		}()

		if err := executeScan(server, lastRowFailsTable, stream); err == nil {
			t.Fatalf("scan %d: Execute returned no error, but the hydrate call for the last row failed", i)
		}
	}
}

// TestExecuteDoesNotLeakGoroutinesWhenEveryRowFails runs scans in which every
// hydrate call fails. Without the fix, every error after the first two blocks
// its sender forever, so the goroutine count grows with each scan.
func TestExecuteDoesNotLeakGoroutinesWhenEveryRowFails(t *testing.T) {
	const (
		scans = 20
		slack = 10
	)
	server := newScanErrorServer(t)
	scan := func() {
		t.Helper()
		if err := executeScan(server, allRowsFailTable, &executeStream{ctx: context.Background()}); err == nil {
			t.Fatal("Execute returned no error, but every hydrate call failed")
		}
	}

	// the first scan starts any long-lived goroutines, so count from after it
	scan()
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	for i := 0; i < scans; i++ {
		scan()
	}

	// goroutines of the last scans may still be finishing
	after := runtime.NumGoroutine()
	for deadline := time.Now().Add(5 * time.Second); after > before+slack && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
		after = runtime.NumGoroutine()
	}
	if after > before+slack {
		t.Fatalf("goroutines grew from %d to %d after %d scans in which every hydrate call failed", before, after, scans)
	}
}
