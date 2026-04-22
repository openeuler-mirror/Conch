package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openeuler/Conch/internal/sandbox"
	"github.com/openeuler/Conch/internal/snapshot"
)

type fakeSandboxManager struct {
	listFn func(req sandbox.SandboxListRequest) ([]sandbox.SandboxRuntimeInfo, error)
	getFn  func(req sandbox.SandboxGetRequest) (*sandbox.SandboxRuntimeInfo, error)
}

func (f *fakeSandboxManager) Create(req sandbox.SandboxCreateRequest) (string, error) {
	return "", nil
}

func (f *fakeSandboxManager) Delete(req sandbox.SandboxDeleteRequest) error {
	return nil
}

func (f *fakeSandboxManager) Pause(req sandbox.SandboxPauseRequest) (string, error) {
	return "", nil
}

func (f *fakeSandboxManager) List(req sandbox.SandboxListRequest) ([]sandbox.SandboxRuntimeInfo, error) {
	if f.listFn == nil {
		return []sandbox.SandboxRuntimeInfo{}, nil
	}
	return f.listFn(req)
}

func (f *fakeSandboxManager) Get(req sandbox.SandboxGetRequest) (*sandbox.SandboxRuntimeInfo, error) {
	if f.getFn == nil {
		return nil, fmt.Errorf("%w: %s", sandbox.ErrSandboxNotFound, req.SandboxId)
	}
	return f.getFn(req)
}

func newTestServer(manager sandboxManager) *Server {
	s := &Server{router: http.NewServeMux()}
	s.SetSandboxManager(manager)
	s.SetSnapshotLister(func(req snapshot.ListRequest) ([]snapshot.SnapshotInfo, error) {
		return []snapshot.SnapshotInfo{}, nil
	})
	s.SetSnapshotGetter(func(req snapshot.GetRequest) (*snapshot.SnapshotInfo, error) {
		return nil, fmt.Errorf("%w: %s", snapshot.ErrSnapshotNotFound, req.SnapshotId)
	})
	s.routes()
	return s
}

func TestHandleListSandboxesReturnsEmptyList(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{
		listFn: func(req sandbox.SandboxListRequest) ([]sandbox.SandboxRuntimeInfo, error) {
			return []sandbox.SandboxRuntimeInfo{}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/sandbox/list", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"sandboxes":[]`) {
		t.Fatalf("expected empty sandboxes array, got body %s", rr.Body.String())
	}

	var resp sandboxListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status ok, got %q", resp.Status)
	}
	if resp.Count != 0 {
		t.Fatalf("expected count 0, got %d", resp.Count)
	}
	if len(resp.Sandboxes) != 0 {
		t.Fatalf("expected no sandboxes, got %d", len(resp.Sandboxes))
	}
}

func TestHandleGetSandboxReturnsInfo(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{
		getFn: func(req sandbox.SandboxGetRequest) (*sandbox.SandboxRuntimeInfo, error) {
			return &sandbox.SandboxRuntimeInfo{
				Namespace: "default",
				SandboxId: req.SandboxId,
				IP:        "10.12.0.5",
				Running:   true,
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/sandbox/get?sandbox_id=sbx-1", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var resp sandboxGetResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" || !resp.Exists {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Sandbox == nil {
		t.Fatal("expected sandbox info in response")
	}
	if resp.Sandbox.SandboxId != "sbx-1" {
		t.Fatalf("expected sandbox_id sbx-1, got %q", resp.Sandbox.SandboxId)
	}
	if resp.Sandbox.IP != "10.12.0.5" {
		t.Fatalf("expected IP 10.12.0.5, got %q", resp.Sandbox.IP)
	}
	if !resp.Sandbox.Running {
		t.Fatal("expected running=true")
	}
}

func TestHandleGetSandboxReturnsNotFound(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{
		getFn: func(req sandbox.SandboxGetRequest) (*sandbox.SandboxRuntimeInfo, error) {
			return nil, fmt.Errorf("%w: %s", sandbox.ErrSandboxNotFound, req.SandboxId)
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/sandbox/get?sandbox_id=missing", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rr.Code)
	}

	var resp sandboxGetResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "not_found" || resp.Exists {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Sandbox != nil {
		t.Fatalf("expected no sandbox payload, got %+v", resp.Sandbox)
	}
}

func TestHandleGetSandboxRequiresSandboxID(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})

	req := httptest.NewRequest(http.MethodGet, "/api/sandbox/get", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

func TestHandleListSandboxesHidesInternalErrors(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{
		listFn: func(req sandbox.SandboxListRequest) ([]sandbox.SandboxRuntimeInfo, error) {
			return nil, fmt.Errorf("secret internal list failure")
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/sandbox/list", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, "secret internal list failure") {
		t.Fatalf("expected internal error details to be hidden, got body %s", body)
	}
	if !strings.Contains(body, "Failed to list sandboxes") {
		t.Fatalf("expected generic error message, got body %s", body)
	}
}

func TestHandleGetSandboxHidesInternalErrors(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{
		getFn: func(req sandbox.SandboxGetRequest) (*sandbox.SandboxRuntimeInfo, error) {
			return nil, fmt.Errorf("secret internal get failure")
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/sandbox/get?sandbox_id=sbx-1", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, "secret internal get failure") {
		t.Fatalf("expected internal error details to be hidden, got body %s", body)
	}
	if !strings.Contains(body, "Failed to get sandbox") {
		t.Fatalf("expected generic error message, got body %s", body)
	}
}

func TestHandleListSnapshotReturnsEmptyList(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})
	s.SetSnapshotLister(func(req snapshot.ListRequest) ([]snapshot.SnapshotInfo, error) {
		return []snapshot.SnapshotInfo{}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/list", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"snapshots":[]`) {
		t.Fatalf("expected empty snapshots array, got body %s", rr.Body.String())
	}

	var resp snapshotListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" || resp.Count != 0 || len(resp.Snapshots) != 0 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHandleListSnapshotReturnsItems(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})
	s.SetSnapshotLister(func(req snapshot.ListRequest) ([]snapshot.SnapshotInfo, error) {
		return []snapshot.SnapshotInfo{{Namespace: "default", SnapshotId: "snap-1"}}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/list?namespace=default", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var resp snapshotListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Count != 1 || len(resp.Snapshots) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Snapshots[0].SnapshotId != "snap-1" || resp.Snapshots[0].Namespace != "default" {
		t.Fatalf("unexpected snapshot item: %+v", resp.Snapshots[0])
	}
}

func TestHandleListSnapshotHidesInternalErrors(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})
	s.SetSnapshotLister(func(req snapshot.ListRequest) ([]snapshot.SnapshotInfo, error) {
		return nil, fmt.Errorf("secret internal snapshot failure")
	})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/list", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, "secret internal snapshot failure") {
		t.Fatalf("expected internal error details to be hidden, got body %s", body)
	}
	if !strings.Contains(body, "Failed to list snapshots") {
		t.Fatalf("expected generic error message, got body %s", body)
	}
}

func TestHandleGetSnapshotReturnsInfo(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})
	s.SetSnapshotGetter(func(req snapshot.GetRequest) (*snapshot.SnapshotInfo, error) {
		return &snapshot.SnapshotInfo{Namespace: "default", SnapshotId: req.SnapshotId}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/get?snapshot_id=snap-1", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var resp snapshotGetResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" || !resp.Exists || resp.Snapshot == nil {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Snapshot.SnapshotId != "snap-1" || resp.Snapshot.Namespace != "default" {
		t.Fatalf("unexpected snapshot payload: %+v", resp.Snapshot)
	}
}

func TestHandleGetSnapshotReturnsNotFound(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})
	s.SetSnapshotGetter(func(req snapshot.GetRequest) (*snapshot.SnapshotInfo, error) {
		return nil, fmt.Errorf("%w: %s", snapshot.ErrSnapshotNotFound, req.SnapshotId)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/get?snapshot_id=missing", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rr.Code)
	}

	var resp snapshotGetResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "not_found" || resp.Exists || resp.Snapshot != nil {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHandleGetSnapshotRequiresSnapshotID(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/get", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

func TestHandleGetSnapshotHidesInternalErrors(t *testing.T) {
	s := newTestServer(&fakeSandboxManager{})
	s.SetSnapshotGetter(func(req snapshot.GetRequest) (*snapshot.SnapshotInfo, error) {
		return nil, fmt.Errorf("secret internal snapshot get failure")
	})

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot/get?snapshot_id=snap-1", nil)
	rr := httptest.NewRecorder()

	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, "secret internal snapshot get failure") {
		t.Fatalf("expected internal error details to be hidden, got body %s", body)
	}
	if !strings.Contains(body, "Failed to get snapshot") {
		t.Fatalf("expected generic error message, got body %s", body)
	}
}
