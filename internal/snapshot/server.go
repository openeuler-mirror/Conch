package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"

	"github.com/openeuler/Conch/internal/daemon"
	"github.com/openeuler/Conch/internal/snapshot/common"
	"github.com/openeuler/Conch/internal/snapshot/snapshotter"
)

// server manages snapshot lifecycle with caching and view sharing.
type server struct {
	snt             snapshotter.Snapshotter
	activeSnapshots map[string]map[string]*snapshots.Info
	lock            sync.RWMutex
	workDir         string
	viewMgr         *viewManager
}

type resumeWorkspacePlan struct {
	rootfs            SnapshotLocator
	mem               SnapshotLocator
	vmViewAliasKey    string
	vmViewSnapshotKey string
}

var gServer server

// NewServer initializes the snapshot server with containerd client.
func NewServer(workDir string, daemonClient *daemon.Client) error {
	sn, err := snapshotter.NewContainerdSnap(daemonClient)
	if err != nil {
		return err
	}
	gServer.snt = sn
	gServer.workDir = workDir
	gServer.activeSnapshots = make(map[string]map[string]*snapshots.Info)
	gServer.viewMgr = &viewManager{
		viewMounts:  make(map[string]map[string]*viewMountRef),
		viewAliases: make(map[string]map[string]string),
	}

	return nil
}

func (s *server) List(req ListRequest) ([]SnapshotInfo, error) {
	ctx := context.Background()
	namespaces := make([]string, 0, 1)
	if req.Namespace != "" {
		namespaces = append(namespaces, req.Namespace)
	} else {
		listed, err := s.snt.ListNamespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		namespaces = append(namespaces, listed...)
	}

	items := make([]SnapshotInfo, 0)
	for _, namespace := range namespaces {
		result := make(map[string]*snapshots.Info)
		if err := s.snt.List(ctx, namespace, result); err != nil {
			return nil, fmt.Errorf("list snapshots in namespace %s: %w", namespace, err)
		}

		for snapshotID, info := range result {
			if !isCommittedUserSnapshot(info) {
				continue
			}
			items = append(items, SnapshotInfo{
				Namespace:  namespace,
				SnapshotId: snapshotID,
			})
		}
	}

	sortSnapshotInfos(items)
	return items, nil
}

func (s *server) Get(req GetRequest) (*SnapshotInfo, error) {
	if req.SnapshotId == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}

	ctx := context.Background()
	namespaces := make([]string, 0, 1)
	if req.Namespace != "" {
		namespaces = append(namespaces, req.Namespace)
	} else {
		listed, err := s.snt.ListNamespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		namespaces = append(namespaces, listed...)
	}

	for _, namespace := range namespaces {
		info, err := s.snt.Stat(ctx, namespace, req.SnapshotId)
		if err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("stat snapshot %s in namespace %s: %w", req.SnapshotId, namespace, err)
		}
		if !isCommittedUserSnapshot(&info) {
			continue
		}
		return &SnapshotInfo{
			Namespace:  namespace,
			SnapshotId: req.SnapshotId,
		}, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, req.SnapshotId)
}

func (s *server) DeleteCommitted(req DeleteRequest) (*DeleteResult, error) {
	if req.SnapshotId == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}

	ctx := context.Background()
	namespaces := make([]string, 0, 1)
	if req.Namespace != "" {
		namespaces = append(namespaces, req.Namespace)
	} else {
		listed, err := s.snt.ListNamespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		namespaces = append(namespaces, listed...)
	}

	for _, namespace := range namespaces {
		result, err := s.deleteCommittedInNamespace(ctx, namespace, req.SnapshotId)
		if err == nil {
			return result, nil
		}
		if errors.Is(err, ErrSnapshotNotFound) {
			continue
		}
		return nil, err
	}

	return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, req.SnapshotId)
}

func (s *server) deleteCommittedInNamespace(ctx context.Context, namespace, snapshotID string) (*DeleteResult, error) {
	info, err := s.snt.Stat(ctx, namespace, snapshotID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
		}
		return nil, fmt.Errorf("stat snapshot %s in namespace %s: %w", snapshotID, namespace, err)
	}
	if !isCommittedUserSnapshot(&info) {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotDeletable, snapshotID)
	}

	memSnapshotID := info.Labels[common.SnapshotLabelMemSnapshot]
	validatedMemSnapshotID, err := s.validateCommittedMemSnapshot(ctx, namespace, snapshotID, memSnapshotID)
	if err != nil {
		return nil, err
	}
	memSnapshotID = validatedMemSnapshotID

	rootfsActiveRefs, memActiveRefs := s.collectActiveSnapshotParentRefs(namespace, snapshotID, memSnapshotID)
	rootfsAliases, memAliases := s.collectViewAliasRefs(namespace, snapshotID, memSnapshotID)
	rootfsViewRef, memViewRef := s.inspectViewMountRefs(namespace, snapshotID, memSnapshotID)
	if len(rootfsActiveRefs) > 0 || len(memActiveRefs) > 0 || len(rootfsAliases) > 0 || len(memAliases) > 0 {
		return nil, fmt.Errorf("%w: snapshot %s has active runtime references", ErrSnapshotInUse, snapshotID)
	}
	if rootfsViewRef.inUse || memViewRef.inUse {
		return nil, fmt.Errorf("%w: snapshot %s has active shared view mounts", ErrSnapshotInUse, snapshotID)
	}

	if rootfsViewRef.zeroRef {
		if err := s.viewMgr.releaseViewMount(s.snt, namespace, snapshotID); err != nil {
			return nil, fmt.Errorf("release rootfs shared view for %s: %w", snapshotID, err)
		}
	}
	if memViewRef.zeroRef {
		if err := s.viewMgr.releaseViewMount(s.snt, namespace, memSnapshotID); err != nil {
			return nil, fmt.Errorf("release mem shared view for %s: %w", memSnapshotID, err)
		}
	}

	children, err := s.collectCommittedSnapshotChildren(ctx, namespace, snapshotID, memSnapshotID)
	if err != nil {
		return nil, err
	}
	if len(children) > 0 {
		return nil, fmt.Errorf("%w: snapshot %s has dependent snapshots %v", ErrSnapshotInUse, snapshotID, children)
	}

	if memSnapshotID != "" {
		if err := s.snt.Remove(ctx, namespace, memSnapshotID); err != nil {
			if !errdefs.IsNotFound(err) {
				if errdefs.IsFailedPrecondition(err) {
					return nil, fmt.Errorf("%w: committed mem snapshot %s is still referenced", ErrSnapshotInUse, memSnapshotID)
				}
				return nil, fmt.Errorf("remove committed mem snapshot %s: %w", memSnapshotID, err)
			}
		}
	}

	if err := s.snt.Remove(ctx, namespace, snapshotID); err != nil {
		if errdefs.IsFailedPrecondition(err) {
			return nil, fmt.Errorf("%w: snapshot %s is still referenced", ErrSnapshotInUse, snapshotID)
		}
		return nil, fmt.Errorf("remove committed snapshot %s: %w", snapshotID, err)
	}

	return &DeleteResult{
		Namespace:     namespace,
		SnapshotId:    snapshotID,
		MemSnapshotId: memSnapshotID,
	}, nil
}

func (s *server) validateCommittedMemSnapshot(ctx context.Context, namespace, rootfsSnapshotID, memSnapshotID string) (string, error) {
	if memSnapshotID == "" {
		return "", nil
	}

	memInfo, err := s.snt.Stat(ctx, namespace, memSnapshotID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("stat mem snapshot %s in namespace %s: %w", memSnapshotID, namespace, err)
	}
	if memInfo.Kind != snapshots.KindCommitted {
		return "", fmt.Errorf("%w: mem snapshot %s is not committed", ErrSnapshotNotDeletable, memSnapshotID)
	}
	if memInfo.Labels == nil || memInfo.Labels[common.SnapshotLabelRootfsSnapshot] != rootfsSnapshotID {
		return "", fmt.Errorf("%w: mem snapshot %s is not associated with rootfs snapshot %s", ErrSnapshotNotDeletable, memSnapshotID, rootfsSnapshotID)
	}
	return memSnapshotID, nil
}

func isCommittedUserSnapshot(info *snapshots.Info) bool {
	if info == nil || info.Kind != snapshots.KindCommitted || info.Labels == nil {
		return false
	}
	if info.Labels[common.SnapshotLabel] != "true" {
		return false
	}
	if info.Labels[common.SnapshotLabelMemSnapshot] == "" {
		return false
	}
	if info.Labels[common.SnapshotLabelVMSnapshot] == "" {
		return false
	}
	return true
}

type viewMountUsage struct {
	zeroRef bool
	inUse   bool
}

func (s *server) collectActiveSnapshotParentRefs(namespace, rootfsSnapshotID, memSnapshotID string) (rootfsRefs, memRefs []string) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for key, info := range s.activeSnapshots[namespace] {
		if info == nil {
			continue
		}
		switch {
		case info.Parent == rootfsSnapshotID:
			rootfsRefs = append(rootfsRefs, key)
		case memSnapshotID != "" && info.Parent == memSnapshotID:
			memRefs = append(memRefs, key)
		}
	}
	return rootfsRefs, memRefs
}

func (s *server) collectViewAliasRefs(namespace, rootfsSnapshotID, memSnapshotID string) (rootfsAliases, memAliases []string) {
	s.viewMgr.viewLock.Lock()
	defer s.viewMgr.viewLock.Unlock()

	for aliasKey, parentSnapshotID := range s.viewMgr.viewAliases[namespace] {
		switch {
		case parentSnapshotID == rootfsSnapshotID:
			rootfsAliases = append(rootfsAliases, aliasKey)
		case memSnapshotID != "" && parentSnapshotID == memSnapshotID:
			memAliases = append(memAliases, aliasKey)
		}
	}
	return rootfsAliases, memAliases
}

func (s *server) inspectViewMountRefs(namespace, rootfsSnapshotID, memSnapshotID string) (rootfsRef, memRef viewMountUsage) {
	s.viewMgr.viewLock.Lock()
	defer s.viewMgr.viewLock.Unlock()

	if nsMap, ok := s.viewMgr.viewMounts[namespace]; ok {
		if ref, ok := nsMap[rootfsSnapshotID]; ok {
			rootfsRef = classifyViewMountUsage(ref)
		}
		if memSnapshotID != "" {
			if ref, ok := nsMap[memSnapshotID]; ok {
				memRef = classifyViewMountUsage(ref)
			}
		}
	}
	return rootfsRef, memRef
}

func classifyViewMountUsage(ref *viewMountRef) viewMountUsage {
	if ref == nil {
		return viewMountUsage{}
	}
	if ref.ready != nil || ref.refCount > 0 {
		return viewMountUsage{inUse: true}
	}
	if ref.refCount == 0 {
		return viewMountUsage{zeroRef: true}
	}
	return viewMountUsage{}
}

func (s *server) collectCommittedSnapshotChildren(ctx context.Context, namespace, rootfsSnapshotID, memSnapshotID string) ([]string, error) {
	result := make(map[string]*snapshots.Info)
	if err := s.snt.List(ctx, namespace, result); err != nil {
		return nil, fmt.Errorf("list snapshots in namespace %s: %w", namespace, err)
	}

	children := make([]string, 0)
	for key, info := range result {
		if info == nil {
			continue
		}
		if info.Parent != rootfsSnapshotID && (memSnapshotID == "" || info.Parent != memSnapshotID) {
			continue
		}
		children = append(children, key)
	}
	return children, nil
}

// getActiveSnapshot retrieves runtime active snapshot info from cache.
func (s *server) getActiveSnapshot(ns, key string) *snapshots.Info {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if m, ok := s.activeSnapshots[ns]; ok {
		return m[key]
	}
	return nil
}

// addActiveSnapshot adds active snapshot info to the runtime cache.
func (s *server) addActiveSnapshot(ns, key string, info *snapshots.Info) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if m, ok := s.activeSnapshots[ns]; ok {
		m[key] = info
	} else {
		m := make(map[string]*snapshots.Info)
		m[key] = info
		s.activeSnapshots[ns] = m
	}
}

// removeActiveSnapshot removes active snapshot info from the runtime cache.
func (s *server) removeActiveSnapshot(ns, key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if m, ok := s.activeSnapshots[ns]; ok {
		delete(m, key)
	}
}

// mkdirAll creates a directory with common.DirMode permissions.
func (s *server) mkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// unmountPath unmounts a filesystem path and removes the directory.
// Skips if path doesn't exist (may have been cleaned up already).
func (s *server) unmountPath(path string) error {
	// Check if path exists first
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	if err := mount.Unmount(path, unix.MNT_FORCE); err != nil {
		// If unmount fails because path doesn't exist, that's ok
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("unmount %s: %w", path, err)
	}
	// Remove the mount point directory after unmount
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove dir %s: %w", path, err)
	}
	if err := cleanupEmptySnapshotParents(path); err != nil {
		return fmt.Errorf("prune empty parent dirs for %s: %w", path, err)
	}
	return nil
}

// Prepare creates active snapshots for rootfs/mem and a shared view for vm.
func (s *server) Prepare(
	ctx context.Context,
	namespace, key string,
	parents ParentSnapshotIDs,
	opts ...Opt,
) (_ *SnapshotConfig, err error) {
	if si := s.getActiveSnapshot(namespace, key); si != nil {
		return nil, fmt.Errorf("snapshot [%s:%s] existed", namespace, key)
	}

	memKey := getMemKeyFromRootfs(key)
	vmViewAliasKey := getVMViewAliasKey(key)
	vmViewSnapshotKey := getSharedViewSnapshotKey(common.SnapshotMountVM, parents.VM)

	ops := &snapshotOps{server: s}

	conf := &SnapshotConfig{
		Rootfs: getActiveMountPath(s.workDir, namespace, key, common.SnapshotMountRootfs),
		MemDir: getActiveMountPath(s.workDir, namespace, key, common.SnapshotMountMem),
		VmDir:  getSharedMountPath(s.workDir, namespace, parents.VM),
	}
	conf.initDefaults()
	for _, o := range opts {
		o(conf)
	}
	conf.createLabels()

	type cleanupItem struct {
		key     string
		cleaner *snapshotCleaner
	}
	var activeCleanups []cleanupItem
	var vmCleaner *snapshotCleaner
	defer func() {
		if err != nil {
			for _, item := range activeCleanups {
				s.removeActiveSnapshot(namespace, item.key)
				if item.cleaner != nil {
					item.cleaner.Cleanup()
				}
			}
			if vmCleaner != nil {
				vmCleaner.Cleanup()
			}
		}
	}()

	// Step 1: prepare rootfs
	rootfsCleaner, err := ops.prepareAndRegisterSnapshot(
		ctx,
		NewSnapshotLocator(namespace, key, parents.Rootfs),
		conf.Rootfs,
		withLabels(conf),
	)
	if err != nil {
		return nil, err
	}
	activeCleanups = append(activeCleanups, cleanupItem{key: key, cleaner: rootfsCleaner})

	conf.pmemFiles, err = listRootfsLayerErofs(conf.Rootfs)
	if err != nil {
		return nil, fmt.Errorf("list rootfs layer erofs failed: %v", err)
	}

	// Step 2: view vm (read-only, shared)
	vmCleaner, err = ops.viewSnapshot(ctx, namespace, parents.VM, vmViewAliasKey, vmViewSnapshotKey, conf.VmDir)
	if err != nil {
		return nil, fmt.Errorf("view vm failed: %v", err)
	}

	// Step 3: prepare mem + create sparse memfile
	memCleaner, err := ops.prepareAndRegisterSnapshot(ctx, NewSnapshotLocator(namespace, memKey, parents.Mem), conf.MemDir)
	if err != nil {
		return nil, err
	}
	activeCleanups = append(activeCleanups, cleanupItem{key: memKey, cleaner: memCleaner})

	if err = ensureMemFile(conf, conf.MemDir, true); err != nil {
		return nil, fmt.Errorf("prepare mem.img failed: %v", err)
	}

	// Step 4: prepare snapshot config files dir
	if err = prepareSnapshotFiles(conf); err != nil {
		return nil, fmt.Errorf("prepare vm snapshot files failed: %v", err)
	}

	return conf, nil
}

// AcquireView views and mounts 3 committed snapshots for snapshot-based startup.
// If already viewed and mounted, reuses existing mounts (refCount++).
func (s *server) AcquireView(
	ctx context.Context,
	namespace, key string,
	parents ParentSnapshotIDs,
	opts ...Opt,
) (_ *SnapshotConfig, err error) {
	rootfsViewAliasKey := getRootfsViewAliasKey(key)
	rootfsViewSnapshotKey := getSharedViewSnapshotKey(common.SnapshotMountRootfs, parents.Rootfs)
	memViewAliasKey := getMemViewAliasKey(key)
	memViewSnapshotKey := getSharedViewSnapshotKey(common.SnapshotMountMem, parents.Mem)
	vmViewAliasKey := getVMViewAliasKey(key)
	vmViewSnapshotKey := getSharedViewSnapshotKey(common.SnapshotMountVM, parents.VM)

	conf := &SnapshotConfig{
		Rootfs: getSharedMountPath(s.workDir, namespace, parents.Rootfs),
		MemDir: getSharedMountPath(s.workDir, namespace, parents.Mem),
		VmDir:  getSharedMountPath(s.workDir, namespace, parents.VM),
	}
	conf.initDefaults()
	for _, o := range opts {
		o(conf)
	}
	conf.createLabels()

	ops := &snapshotOps{server: s}

	var cleanups []*snapshotCleaner
	defer func() {
		if err != nil {
			for _, c := range cleanups {
				if c != nil {
					c.Cleanup()
				}
			}
		}
	}()

	// Step 1: view rootfs
	rootfsCleaner, err := ops.viewSnapshot(ctx, namespace, parents.Rootfs, rootfsViewAliasKey, rootfsViewSnapshotKey, conf.Rootfs, withLabels(conf))
	if err != nil {
		return nil, fmt.Errorf("view rootfs failed: %v", err)
	}
	cleanups = append(cleanups, rootfsCleaner)

	conf.pmemFiles, err = listRootfsLayerErofs(conf.Rootfs)
	if err != nil {
		return nil, fmt.Errorf("list rootfs layer erofs failed: %v", err)
	}

	// Step 2: view vm
	vmCleaner, err := ops.viewSnapshot(ctx, namespace, parents.VM, vmViewAliasKey, vmViewSnapshotKey, conf.VmDir)
	if err != nil {
		return nil, fmt.Errorf("view vm failed: %v", err)
	}
	cleanups = append(cleanups, vmCleaner)

	// Step 3: view mem + verify mem.img
	memCleaner, err := ops.viewSnapshot(ctx, namespace, parents.Mem, memViewAliasKey, memViewSnapshotKey, conf.MemDir)
	if err != nil {
		return nil, fmt.Errorf("view mem failed: %v", err)
	}
	cleanups = append(cleanups, memCleaner)

	if err = ensureMemFile(conf, conf.MemDir, false); err != nil {
		return nil, fmt.Errorf("mem.img verification failed: %v", err)
	}

	return conf, nil
}

// AcquireResumeWorkspace prepares a restore workspace for snapshot-based startup.
// Rootfs and mem are prepared as active layers so a resumed sandbox can later be
// committed again, while VM remains a shared view.
func (s *server) AcquireResumeWorkspace(
	ctx context.Context,
	namespace, key string,
	parents ParentSnapshotIDs,
	cid uint32,
	socketPath string,
	opts ...Opt,
) (_ *SnapshotConfig, err error) {
	memKey := getMemKeyFromRootfs(key)
	plan := buildResumeWorkspacePlan(namespace, key, parents)
	conf := newResumeWorkspaceConfig(s.workDir, namespace, key, parents)
	conf.initDefaults()
	rootfsInfo, err := s.snt.Stat(ctx, namespace, parents.Rootfs)
	if err != nil {
		return nil, fmt.Errorf("stat rootfs snapshot %s failed: %w", parents.Rootfs, err)
	}
	mergeLabels(&rootfsInfo, conf)
	for _, o := range opts {
		o(conf)
	}
	nextSnapshotID, err := CalculateSnapshotID(namespace, key, parents.Rootfs)
	if err != nil {
		return nil, fmt.Errorf("calculate next snapshot id failed: %w", err)
	}
	conf.NextSnapshotRoot = nextSnapshotRootDir(nextSnapshotID)
	conf.createLabels()

	ops := &snapshotOps{server: s}

	type cleanupItem struct {
		key     string
		cleaner *snapshotCleaner
	}
	var activeCleanups []cleanupItem
	var viewCleanups []*snapshotCleaner
	defer func() {
		if err != nil {
			for _, item := range activeCleanups {
				s.removeActiveSnapshot(namespace, item.key)
				if item.cleaner != nil {
					item.cleaner.Cleanup()
				}
			}
			for _, cleaner := range viewCleanups {
				if cleaner != nil {
					cleaner.Cleanup()
				}
			}
		}
	}()

	rootfsCleaner, err := ops.prepareAndRegisterSnapshot(
		ctx,
		plan.rootfs,
		conf.Rootfs,
		withLabels(conf),
	)
	if err != nil {
		return nil, fmt.Errorf("prepare rootfs failed: %v", err)
	}
	activeCleanups = append(activeCleanups, cleanupItem{key: key, cleaner: rootfsCleaner})

	conf.pmemFiles, err = listRootfsLayerErofs(conf.Rootfs)
	if err != nil {
		return nil, fmt.Errorf("list rootfs layer erofs failed: %v", err)
	}

	vmCleaner, err := ops.viewSnapshot(ctx, namespace, parents.VM, plan.vmViewAliasKey, plan.vmViewSnapshotKey, conf.VmDir)
	if err != nil {
		return nil, fmt.Errorf("view vm failed: %v", err)
	}
	viewCleanups = append(viewCleanups, vmCleaner)

	memCleaner, err := ops.prepareAndRegisterSnapshot(ctx, plan.mem, conf.MemDir)
	if err != nil {
		return nil, err
	}
	activeCleanups = append(activeCleanups, cleanupItem{key: memKey, cleaner: memCleaner})

	if err = ensureMemFile(conf, conf.MemDir, false); err != nil {
		return nil, fmt.Errorf("mem.img verification failed: %v", err)
	}

	configUpdater := &configUpdater{}
	configFilePath := filepath.Join(conf.CurrentSnapshotDir(), common.SnapshotConfigFileName)
	if err = configUpdater.updateSnapshotConfig(
		configFilePath,
		conf.KernelFile(),
		conf.InitrdFile(),
		conf.SnapshotMemFile(),
		conf.PmemFiles(),
		cid,
		socketPath,
	); err != nil {
		return nil, fmt.Errorf("update snapshot config failed: %v", err)
	}

	if err = prepareSnapshotFiles(conf); err != nil {
		return nil, fmt.Errorf("prepare next vm snapshot files failed: %v", err)
	}

	return conf, nil
}

func buildResumeWorkspacePlan(namespace, key string, parents ParentSnapshotIDs) resumeWorkspacePlan {
	return resumeWorkspacePlan{
		rootfs:            NewSnapshotLocator(namespace, key, parents.Rootfs),
		mem:               NewSnapshotLocator(namespace, getMemKeyFromRootfs(key), parents.Mem),
		vmViewAliasKey:    getVMViewAliasKey(key),
		vmViewSnapshotKey: getSharedViewSnapshotKey(common.SnapshotMountVM, parents.VM),
	}
}

func newResumeWorkspaceConfig(workDir, namespace, key string, parents ParentSnapshotIDs) *SnapshotConfig {
	return &SnapshotConfig{
		Rootfs: getActiveMountPath(workDir, namespace, key, common.SnapshotMountRootfs),
		MemDir: getActiveMountPath(workDir, namespace, key, common.SnapshotMountMem),
		VmDir:  getSharedMountPath(workDir, namespace, parents.VM),
	}
}

func (s *server) resolveParentSnapshotIDs(namespace, rootfs string, allowEmptyMem bool) (ParentSnapshotIDs, error) {
	if rootfs == "" {
		return ParentSnapshotIDs{}, nil
	}
	info, err := s.snt.Stat(context.Background(), namespace, rootfs)
	if err != nil {
		return ParentSnapshotIDs{}, fmt.Errorf("rootfs snapshot %s not found (maybe not unpacked): %v", rootfs, err)
	}
	parentMem, ok := info.Labels[common.SnapshotLabelMemSnapshot]
	if (!ok || parentMem == "") && !allowEmptyMem {
		return ParentSnapshotIDs{}, fmt.Errorf("mem snapshot label not found on rootfs snapshot %s", rootfs)
	}
	parentVM, ok := info.Labels[common.SnapshotLabelVMSnapshot]
	if !ok || parentVM == "" {
		return ParentSnapshotIDs{}, fmt.Errorf("vm snapshot label not found on rootfs snapshot %s", rootfs)
	}
	return ParentSnapshotIDs{
		Rootfs: rootfs,
		Mem:    parentMem,
		VM:     parentVM,
	}, nil
}

// ResolveParentSnapshotIDs resolves parent mem/vm snapshots from a committed rootfs snapshot.
// This is the strict path used for snapshot-based startup and requires both mem/vm labels.
func (s *server) ResolveParentSnapshotIDs(namespace, rootfs string) (ParentSnapshotIDs, error) {
	return s.resolveParentSnapshotIDs(namespace, rootfs, false)
}

// ResolveImageParentSnapshotIDs resolves image startup parents from a rootfs snapshot.
// For image startup, mem snapshot label is optional and an empty mem parent means
// a fresh writable mem layer will be prepared for the sandbox.
func (s *server) ResolveImageParentSnapshotIDs(namespace, rootfs string) (ParentSnapshotIDs, error) {
	return s.resolveParentSnapshotIDs(namespace, rootfs, true)
}

// Commit commits an active snapshot with externally calculated snapshotID.
func (s *server) Commit(ctx context.Context, namespace, snapshotID, key string, opts ...Opt) error {
	si := s.getActiveSnapshot(namespace, key)
	if si == nil {
		return fmt.Errorf("snapshot [%s:%s] not found", namespace, key)
	}

	memKey := getMemKeyFromRootfs(key)
	memInfo := s.getActiveSnapshot(namespace, memKey)
	if memInfo == nil {
		return fmt.Errorf("mem snapshot [%s:%s] not found", namespace, memKey)
	}
	vmViewAliasKey := getVMViewAliasKey(key)
	parentVMSnapshotID, ok := s.viewMgr.getViewAlias(namespace, vmViewAliasKey)
	if !ok {
		return fmt.Errorf("vm view alias [%s:%s] not found", namespace, vmViewAliasKey)
	}

	memSnapshotID, err := CalculateSnapshotID(namespace, memKey, memInfo.Parent)
	if err != nil {
		return fmt.Errorf("calculate mem snapshot id failed: %v", err)
	}

	if snapshotID == "" {
		return fmt.Errorf("rootfs snapshot id is required (compute externally)")
	}

	ops := &snapshotOps{server: s}
	conf, viewConf, err := ops.buildCommitConfigs(ctx, namespace, key, memKey, snapshotID, memSnapshotID, parentVMSnapshotID, si, opts)
	if err != nil {
		return err
	}

	configUpdater := &configUpdater{}
	configFilePath := filepath.Join(conf.SnapDir(), common.SnapshotConfigFileName)
	if err := configUpdater.updateSnapshotConfig(configFilePath, viewConf.KernelFile(), viewConf.InitrdFile(), viewConf.SnapshotMemFile(), viewConf.PmemFiles(), 0, ""); err != nil {
		return fmt.Errorf("update snapshot config failed: %v", err)
	}

	if err := ops.commitRootfsSnapshot(ctx, namespace, key, snapshotID, conf, memSnapshotID, parentVMSnapshotID); err != nil {
		return err
	}

	if err := ops.commitMemSnapshot(ctx, namespace, memKey, memSnapshotID, snapshotID); err != nil {
		return err
	}
	if err := ops.prewarmViewMounts(ctx, namespace, snapshotID, parentVMSnapshotID, viewConf); err != nil {
		return err
	}

	return nil
}

// Remove removes snapshots and cleans up associated resources.
func (s *server) Remove(ctx context.Context, namespace, key string) error {
	memKey := getMemKeyFromRootfs(key)
	rootfsViewAliasKey := getRootfsViewAliasKey(key)
	memViewAliasKey := getMemViewAliasKey(key)
	vmViewAliasKey := getVMViewAliasKey(key)

	type activeItem struct {
		key  string
		info *snapshots.Info
	}
	var activeItems []activeItem
	var viewKeys []string
	var missingKeys []string
	var errs []error

	if info := s.getActiveSnapshot(namespace, key); info != nil {
		activeItems = append(activeItems, activeItem{key: key, info: info})
	} else if _, ok := s.viewMgr.getViewAlias(namespace, rootfsViewAliasKey); ok {
		viewKeys = append(viewKeys, rootfsViewAliasKey)
	} else {
		missingKeys = append(missingKeys, key)
	}

	if info := s.getActiveSnapshot(namespace, memKey); info != nil {
		activeItems = append(activeItems, activeItem{key: memKey, info: info})
	} else if _, ok := s.viewMgr.getViewAlias(namespace, memViewAliasKey); ok {
		viewKeys = append(viewKeys, memViewAliasKey)
	} else {
		missingKeys = append(missingKeys, memKey)
	}

	if _, ok := s.viewMgr.getViewAlias(namespace, vmViewAliasKey); ok {
		viewKeys = append(viewKeys, vmViewAliasKey)
	} else {
		missingKeys = append(missingKeys, vmViewAliasKey)
	}

	if len(activeItems) == 0 && len(viewKeys) == 0 {
		return fmt.Errorf("snapshots [%s:%s,%s] and view aliases [%s,%s,%s] not found in active/view caches", namespace, key, memKey, rootfsViewAliasKey, memViewAliasKey, vmViewAliasKey)
	}

	var unmountErrs []error
	ops := &snapshotOps{server: s}
	for _, item := range activeItems {
		mountPoint := ""
		if item.key == key {
			mountPoint = getActiveMountPath(s.workDir, namespace, key, common.SnapshotMountRootfs)
			if rootfs, ok := item.info.Labels[common.SnapshotLabelRootfs]; ok && rootfs != "" {
				mountPoint = rootfs
			}
		} else if item.key == memKey {
			mountPoint = getActiveMountPath(s.workDir, namespace, key, common.SnapshotMountMem)
		}
		if err := ops.unmountPath(mountPoint); err != nil {
			unmountErrs = append(unmountErrs, err)
		}
	}

	// If unmount failed, don't remove/release to avoid inconsistent state.
	if len(unmountErrs) > 0 {
		return fmt.Errorf("unmount failed, skip cleanup to avoid orphaned dirs: %w", errors.Join(unmountErrs...))
	}

	if len(viewKeys) > 0 {
		if _, releaseErr := s.viewMgr.releaseViewAliases(s.snt, namespace, viewKeys...); releaseErr != nil {
			errs = append(errs, releaseErr)
		}
	}

	for _, item := range activeItems {
		if err := ops.tryRemoveSnapshot(ctx, namespace, item.key); err != nil {
			errs = append(errs, err)
		}
	}
	if len(missingKeys) > 0 {
		errs = append(errs, fmt.Errorf("some snapshot keys not found in active/view caches: %v", missingKeys))
	}

	return errors.Join(errs...)
}

// CleanupAllViews unmounts and removes all view snapshots.
// Should be called during graceful shutdown before Close().
func (s *server) CleanupAllViews() {
	s.viewMgr.CleanupAllViews(s.snt)
}

// Close releases snapshot resources.
func (s *server) Close() error {
	return nil
}

// withLabels creates a snapshot option with labels from config.
func withLabels(conf *SnapshotConfig) snapshots.Opt {
	return func(info *snapshots.Info) error {
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}
		for k, v := range conf.Labels {
			info.Labels[k] = v
		}
		return nil
	}
}
