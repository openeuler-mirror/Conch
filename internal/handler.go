package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/containerd/containerd"
	"github.com/openeuler/Conch/internal/config"
	"github.com/openeuler/Conch/internal/daemon"
	"github.com/openeuler/Conch/internal/sandbox"
	"github.com/openeuler/Conch/internal/sandbox/network"
	"github.com/openeuler/Conch/internal/snapshot"
	"github.com/openeuler/Conch/pkg/ulog"
)

const (
	shutdownTimeout = 30 * time.Second
)

type Server struct {
	router         *http.ServeMux
	sandboxManager sandboxManager
	listSnapshots  func(req snapshot.ListRequest) ([]snapshot.SnapshotInfo, error)
	getSnapshot    func(req snapshot.GetRequest) (*snapshot.SnapshotInfo, error)
	deleteSnapshot func(req snapshot.DeleteRequest) (*snapshot.DeleteResult, error)
	daemonClient   *daemon.Client
	httpServer     *http.Server
	listener       net.Listener
	unixSocketPath string
	cleanupOnce    sync.Once

	// TODO: need ListCachedBuilds()
}

type sandboxManager interface {
	Create(req sandbox.SandboxCreateRequest) (string, error)
	Delete(req sandbox.SandboxDeleteRequest) error
	Pause(req sandbox.SandboxPauseRequest) (string, error)
	List(req sandbox.SandboxListRequest) ([]sandbox.SandboxRuntimeInfo, error)
	Get(req sandbox.SandboxGetRequest) (*sandbox.SandboxRuntimeInfo, error)
}

type sandboxListResponse struct {
	Status    string                       `json:"status"`
	Count     int                          `json:"count"`
	Sandboxes []sandbox.SandboxRuntimeInfo `json:"sandboxes"`
}

type sandboxGetResponse struct {
	Status  string                      `json:"status"`
	Exists  bool                        `json:"exists"`
	Sandbox *sandbox.SandboxRuntimeInfo `json:"sandbox,omitempty"`
}

type snapshotListResponse struct {
	Status    string                  `json:"status"`
	Count     int                     `json:"count"`
	Snapshots []snapshot.SnapshotInfo `json:"snapshots"`
}

type snapshotGetResponse struct {
	Status   string                 `json:"status"`
	Exists   bool                   `json:"exists"`
	Snapshot *snapshot.SnapshotInfo `json:"snapshot,omitempty"`
}

type snapshotDeleteResponse struct {
	Status        string `json:"status"`
	Deleted       bool   `json:"deleted,omitempty"`
	Exists        bool   `json:"exists,omitempty"`
	SnapshotId    string `json:"snapshot_id,omitempty"`
	MemSnapshotId string `json:"mem_snapshot_id,omitempty"`
}

func handleSignals(ctx context.Context, cancel context.CancelFunc, s *Server) {
	go func() {
		var sig os.Signal
		var handledSignals = []os.Signal{
			unix.SIGTERM,
			unix.SIGINT,
		}

		// Do not print message when dealing with SIGPIPE, which may cause
		// nested signals and consume lots of cpu bandwidth.
		signal.Ignore(unix.SIGPIPE)

		signalChannel := make(chan os.Signal, 1)
		signal.Notify(signalChannel, handledSignals...)

		for {
			select {
			case <-ctx.Done():
				ulog.Warn("Context done",
					ulog.F("error", ctx.Err()),
				)
			case sig = <-signalChannel:
				ulog.Info("Interrupted by signal, process exiting",
					ulog.F("signal", sig),
				)
				cancel()
				s.Cleanup()

				return
			}
		}
	}()
	return
}

func NewServer(cfg *config.Config) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		router:         http.NewServeMux(),
		listSnapshots:  snapshot.List,
		getSnapshot:    snapshot.Get,
		deleteSnapshot: snapshot.DeleteCommitted,
	}
	s.routes()

	logger := ulog.GetLogger()

	daemonClient, err := daemon.New(
		cfg.Containerd.Socket,
		containerd.WithDefaultNamespace(cfg.Containerd.DefaultNamespace),
	)
	if err != nil {
		logger.Error("Failed to init containerd manager", ulog.F("error", err))
		cancel()
		return nil, fmt.Errorf("failed to init containerd manager: %w", err)
	}
	s.daemonClient = daemonClient

	// Initialize snapshot server
	err = snapshot.NewServer(cfg.Server.WorkDir, daemonClient)
	if err != nil {
		_ = daemonClient.Close()
		cancel()
		logger.Error("Failed to init snapshot manager", ulog.F("error", err))
		return nil, fmt.Errorf("failed to init snapshot manager: %w", err)
	}

	// Initialize sandbox manager
	pool, err := network.NewPool(cfg.Network.PoolSize, cfg.Network.DynamicReservation, cfg.Network.TapIP, cfg.Network.TapMask)
	if err != nil {
		logger.Error("Failed to initialize network pool; sandbox APIs will return errors", ulog.F("error", err))
		_ = daemonClient.Close()
		cancel()
		_ = snapshot.Close()
		return nil, fmt.Errorf("failed to init network pool: %w", err)
	}

	s.SetSandboxManager(sandbox.NewManager(pool, daemonClient, cfg.Sandbox.VsockSignalRetry, cfg.Sandbox.VsockSignalTimeout, cfg.Sandbox.RequestTimeout))
	go pool.Populate(ctx)

	handleSignals(ctx, cancel, s)

	logger.Info("Server initialized successfully")
	return s, nil
}

func (s *Server) SetSandboxManager(manager sandboxManager) {
	s.sandboxManager = manager
}

func (s *Server) SetSnapshotLister(listFn func(req snapshot.ListRequest) ([]snapshot.SnapshotInfo, error)) {
	s.listSnapshots = listFn
}

func (s *Server) SetSnapshotGetter(getFn func(req snapshot.GetRequest) (*snapshot.SnapshotInfo, error)) {
	s.getSnapshot = getFn
}

func (s *Server) SetSnapshotDeleter(deleteFn func(req snapshot.DeleteRequest) (*snapshot.DeleteResult, error)) {
	s.deleteSnapshot = deleteFn
}

func (s *Server) routes() {
	// sandbox
	s.router.HandleFunc("/api/sandbox/create", s.handleCreateSandbox)
	s.router.HandleFunc("/api/sandbox/delete", s.handleDeleteSandbox)
	s.router.HandleFunc("/api/sandbox/pause", s.handlePauseSandbox)
	s.router.HandleFunc("/api/sandbox/list", s.handleListSandboxes)
	s.router.HandleFunc("/api/sandbox/get", s.handleGetSandbox)
	s.router.HandleFunc("/api/snapshot/list", s.handleListSnapshot)
	s.router.HandleFunc("/api/snapshot/get", s.handleGetSnapshot)
	s.router.HandleFunc("/api/snapshot/delete", s.handleDeleteSnapshot)
}

func (s *Server) Start(addr string, unixSocket string) error {
	logger := ulog.GetLogger()
	var (
		err error
		ln  net.Listener
	)

	if unixSocket != "" {
		// If the Unix socket is not empty, then we should use it for server listen port
		// First create the parent directory if needed; this requires permission for the socket path.
		// Then for any existing stale socket it should be removed before start to listen
		if err := os.MkdirAll(filepath.Dir(unixSocket), 0o755); err != nil {
			return fmt.Errorf("failed to create unix socket directory: %w", err)
		}
		if err := os.Remove(unixSocket); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove stale unix socket: %w", err)
		}

		ln, err = net.Listen("unix", unixSocket)
		if err != nil {
			return fmt.Errorf("failed to listen on unix socket %s: %w", unixSocket, err)
		}
		if err := os.Chmod(unixSocket, 0o660); err != nil {
			_ = ln.Close()
			_ = os.Remove(unixSocket)
			return fmt.Errorf("failed to set unix socket permissions: %w", err)
		}

		s.unixSocketPath = unixSocket
		logger.Info("Starting HTTP server", ulog.F("network", "unix"), ulog.F("socket", unixSocket))
	} else {
		// If the Unix socket is empty, then we should use tcp IP for server listen port
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on address %s: %w", addr, err)
		}
		logger.Info("Starting HTTP server", ulog.F("network", "tcp"), ulog.F("address", addr))
	}

	s.listener = ln
	s.httpServer = &http.Server{Handler: s.router}
	err = s.httpServer.Serve(ln)
	if err == http.ErrServerClosed {
		logger.Info("Main server gracefully stopped")
		err = nil
	}
	return err
}

func (s *Server) Cleanup() {
	logger := ulog.GetLogger()

	s.cleanupOnce.Do(func() {
		// stop httpServer
		if s.httpServer != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer shutdownCancel()
			if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
				logger.Error("HTTP server shutdown error", ulog.F("error", err))
			} else {
				logger.Info("HTTP server gracefully stopped")
			}
		}

		if s.unixSocketPath != "" {
			if err := os.Remove(s.unixSocketPath); err != nil && !os.IsNotExist(err) {
				logger.Error("Failed to remove unix socket", ulog.F("socket", s.unixSocketPath), ulog.F("error", err))
			} else {
				logger.Info("Removed unix socket", ulog.F("socket", s.unixSocketPath))
			}
		}

		if m, ok := s.sandboxManager.(*sandbox.Manager); ok {
			if err := m.CleanupPool(); err != nil {
				logger.Error("Server cleanup error", ulog.F("error", err))
			}
			if err := m.CleanupCIDMap(); err != nil {
				logger.Error("CID map cleanup error", ulog.F("error", err))
			}
		}
		snapshot.CleanupAllViews()
		if err := snapshot.Close(); err != nil {
			logger.Error("Snapshot cleanup error", ulog.F("error", err))
		}
		if err := s.daemonClient.Close(); err != nil {
			logger.Error("Containerd cleanup error", ulog.F("error", err))
		}
		logger.Info("Cleanup completed")
	})
}

func (s *Server) handleCreateSandbox(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling create sandbox request")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req = sandbox.SandboxCreateRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("Invalid request body", ulog.F("error", err))
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	peerIP, err := s.sandboxManager.Create(req)
	if err != nil {
		logger.Error("Failed to create sandbox",
			ulog.F("sandbox_id", req.SandboxId),
			ulog.F("error", err),
		)
		http.Error(w, "Failed to create sandbox: "+err.Error(), http.StatusInternalServerError)
		return
	}

	logger.Info("Sandbox created successfully",
		ulog.F("sandbox_id", req.SandboxId),
		ulog.F("peer_ip", peerIP),
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"ip":     peerIP,
	})
}

func (s *Server) handleDeleteSandbox(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling delete sandbox request")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sandbox.SandboxDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("Invalid request body", ulog.F("error", err))
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	err := s.sandboxManager.Delete(req)
	if err != nil {
		logger.Error("Failed to delete sandbox",
			ulog.F("sandbox_id", req.SandboxId),
			ulog.F("error", err),
		)
		http.Error(w, "Failed to delete sandbox: "+err.Error(), http.StatusInternalServerError)
		return
	}

	logger.Info("Sandbox deleted successfully", ulog.F("sandbox_id", req.SandboxId))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handlePauseSandbox(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling pause sandbox request")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sandbox.SandboxPauseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("Invalid request body", ulog.F("error", err))
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	snapshotId, err := s.sandboxManager.Pause(req)
	if err != nil {
		logger.Error("Failed to pause sandbox",
			ulog.F("sandbox_id", req.SandboxId),
			ulog.F("error", err),
		)
		http.Error(w, "Failed to pause sandbox: "+err.Error(), http.StatusInternalServerError)
		return
	}

	logger.Info("Sandbox paused successfully",
		ulog.F("sandbox_id", req.SandboxId),
		ulog.F("snapshot_id", snapshotId),
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":     "ok",
		"snapshotId": snapshotId,
	})
}

func (s *Server) handleListSandboxes(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling list sandboxes request")

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	infos, err := s.sandboxManager.List(sandbox.SandboxListRequest{
		Namespace: r.URL.Query().Get("namespace"),
	})
	if err != nil {
		logger.Error("Failed to list sandboxes", ulog.F("error", err))
		http.Error(w, "Failed to list sandboxes", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sandboxListResponse{
		Status:    "ok",
		Count:     len(infos),
		Sandboxes: infos,
	})
}

func (s *Server) handleGetSandbox(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling get sandbox request")

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sandboxID := r.URL.Query().Get("sandbox_id")
	if sandboxID == "" {
		http.Error(w, "sandbox_id is required", http.StatusBadRequest)
		return
	}

	info, err := s.sandboxManager.Get(sandbox.SandboxGetRequest{
		Namespace: r.URL.Query().Get("namespace"),
		SandboxId: sandboxID,
	})
	if err != nil {
		if errors.Is(err, sandbox.ErrSandboxNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(sandboxGetResponse{
				Status: "not_found",
				Exists: false,
			})
			return
		}

		logger.Error("Failed to get sandbox",
			ulog.F("sandbox_id", sandboxID),
			ulog.F("error", err),
		)
		http.Error(w, "Failed to get sandbox", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sandboxGetResponse{
		Status:  "ok",
		Exists:  true,
		Sandbox: info,
	})
}

func (s *Server) handleListSnapshot(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling list snapshot request")

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.listSnapshots == nil {
		logger.Error("Snapshot lister is not configured")
		http.Error(w, "Failed to list snapshots", http.StatusInternalServerError)
		return
	}

	items, err := s.listSnapshots(snapshot.ListRequest{
		Namespace: r.URL.Query().Get("namespace"),
	})
	if err != nil {
		logger.Error("Failed to list snapshots", ulog.F("error", err))
		http.Error(w, "Failed to list snapshots", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshotListResponse{
		Status:    "ok",
		Count:     len(items),
		Snapshots: items,
	})
}

func (s *Server) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling get snapshot request")

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshotID := r.URL.Query().Get("snapshot_id")
	if snapshotID == "" {
		http.Error(w, "snapshot_id is required", http.StatusBadRequest)
		return
	}

	if s.getSnapshot == nil {
		logger.Error("Snapshot getter is not configured")
		http.Error(w, "Failed to get snapshot", http.StatusInternalServerError)
		return
	}

	item, err := s.getSnapshot(snapshot.GetRequest{
		Namespace:  r.URL.Query().Get("namespace"),
		SnapshotId: snapshotID,
	})
	if err != nil {
		if errors.Is(err, snapshot.ErrSnapshotNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(snapshotGetResponse{
				Status: "not_found",
				Exists: false,
			})
			return
		}

		logger.Error("Failed to get snapshot",
			ulog.F("snapshot_id", snapshotID),
			ulog.F("error", err),
		)
		http.Error(w, "Failed to get snapshot", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshotGetResponse{
		Status:   "ok",
		Exists:   true,
		Snapshot: item,
	})
}

func (s *Server) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	logger := ulog.GetLogger()
	logger.Debug("Handling delete snapshot request")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.deleteSnapshot == nil {
		logger.Error("Snapshot deleter is not configured")
		http.Error(w, "Failed to delete snapshot", http.StatusInternalServerError)
		return
	}

	var req snapshot.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("Invalid request body", ulog.F("error", err))
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.SnapshotId == "" {
		http.Error(w, "snapshot_id is required", http.StatusBadRequest)
		return
	}

	result, err := s.deleteSnapshot(req)
	if err != nil {
		switch {
		case errors.Is(err, snapshot.ErrSnapshotNotFound):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(snapshotDeleteResponse{
				Status: "not_found",
				Exists: false,
			})
			return
		case errors.Is(err, snapshot.ErrSnapshotInUse):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(snapshotDeleteResponse{Status: "in_use"})
			return
		case errors.Is(err, snapshot.ErrSnapshotNotDeletable):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(snapshotDeleteResponse{Status: "not_deletable"})
			return
		default:
			logger.Error("Failed to delete snapshot",
				ulog.F("snapshot_id", req.SnapshotId),
				ulog.F("error", err),
			)
			http.Error(w, "Failed to delete snapshot", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshotDeleteResponse{
		Status:        "ok",
		Deleted:       true,
		SnapshotId:    result.SnapshotId,
		MemSnapshotId: result.MemSnapshotId,
	})
}
