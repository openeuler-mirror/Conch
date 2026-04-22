package snapshot

import (
	"testing"

	"github.com/containerd/containerd/snapshots"
	"github.com/openeuler/Conch/internal/snapshot/common"
)

func TestCalculateSnapshotIDUsesParentForChainedMemSnapshots(t *testing.T) {
	withoutParent, err := CalculateSnapshotID("default", "sbx-1-mem", "")
	if err != nil {
		t.Fatalf("unexpected error without parent: %v", err)
	}
	withParent, err := CalculateSnapshotID("default", "sbx-1-mem", "sha256:parent-mem")
	if err != nil {
		t.Fatalf("unexpected error with parent: %v", err)
	}
	if withoutParent == withParent {
		t.Fatalf("expected chained mem snapshot id to differ when parent is set, got %q", withParent)
	}
}

func TestBuildResumeWorkspacePlanUsesActiveRootfsLayer(t *testing.T) {
	parents := ParentSnapshotIDs{
		Rootfs: "rootfs-parent",
		Mem:    "mem-parent",
		VM:     "vm-parent",
	}

	plan := buildResumeWorkspacePlan("default", "sbx-1", parents)

	if plan.rootfs.Namespace != "default" || plan.rootfs.Key != "sbx-1" || plan.rootfs.Parent != "rootfs-parent" {
		t.Fatalf("unexpected rootfs locator: %+v", plan.rootfs)
	}
	if plan.mem.Namespace != "default" || plan.mem.Key != "sbx-1-mem" || plan.mem.Parent != "mem-parent" {
		t.Fatalf("unexpected mem locator: %+v", plan.mem)
	}
	if plan.vmViewAliasKey != getVMViewAliasKey("sbx-1") {
		t.Fatalf("unexpected vm view alias key: %q", plan.vmViewAliasKey)
	}
	if plan.vmViewSnapshotKey != getSharedViewSnapshotKey(common.SnapshotMountVM, "vm-parent") {
		t.Fatalf("unexpected vm view snapshot key: %q", plan.vmViewSnapshotKey)
	}
}

func TestNewResumeWorkspaceConfigUsesActiveRootfsPath(t *testing.T) {
	parents := ParentSnapshotIDs{VM: "vm-parent"}

	conf := newResumeWorkspaceConfig("/tmp/conch", "default", "sbx-1", parents)

	if conf.Rootfs != getActiveMountPath("/tmp/conch", "default", "sbx-1", common.SnapshotMountRootfs) {
		t.Fatalf("expected active rootfs path, got %q", conf.Rootfs)
	}
	if conf.MemDir != getActiveMountPath("/tmp/conch", "default", "sbx-1", common.SnapshotMountMem) {
		t.Fatalf("expected active mem path, got %q", conf.MemDir)
	}
	if conf.VmDir != getSharedMountPath("/tmp/conch", "default", "vm-parent") {
		t.Fatalf("expected shared vm path, got %q", conf.VmDir)
	}
}

func TestSnapshotConfigSnapDirUsesNextSnapshotRootWhenSet(t *testing.T) {
	conf := &SnapshotConfig{
		MemDir:           "/tmp/conch/snapshot/default/sbx-1/mem",
		RootDir:          "/conch/snapshot",
		NextSnapshotRoot: nextSnapshotRootDir("sbx-1"),
	}

	if conf.CurrentSnapshotDir() != "/tmp/conch/snapshot/default/sbx-1/mem/conch/snapshot" {
		t.Fatalf("unexpected current snapshot dir: %q", conf.CurrentSnapshotDir())
	}
	if conf.SnapDir() != "/tmp/conch/snapshot/default/sbx-1/mem/conch/snapshot/sbx-1" {
		t.Fatalf("unexpected next snapshot dir: %q", conf.SnapDir())
	}
}

func TestCreateLabelsUsesNextSnapshotRoot(t *testing.T) {
	conf := &SnapshotConfig{
		Rootfs:           "/tmp/rootfs",
		RootDir:          "/conch/snapshot",
		NextSnapshotRoot: nextSnapshotRootDir("sbx-1"),
		Labels:           map[string]string{},
		MemSize:          512,
	}

	conf.createLabels()

	if got := conf.Labels[common.SnapshotLabelSnapshotDir]; got != nextSnapshotRootDir("sbx-1") {
		t.Fatalf("unexpected snapshot dir label: %q", got)
	}
}

func TestMergeLabelsPropagatesNextSnapshotRoot(t *testing.T) {
	conf := &SnapshotConfig{
		Labels: map[string]string{},
	}
	mergeLabels(&snapshots.Info{Labels: map[string]string{
		common.SnapshotLabelSnapshotDir: "conch/snapshot/sbx-2",
	}}, conf)

	if conf.RootDir != "conch/snapshot/sbx-2" {
		t.Fatalf("unexpected root dir after merge: %q", conf.RootDir)
	}
	if conf.NextSnapshotRoot != "conch/snapshot/sbx-2" {
		t.Fatalf("unexpected next snapshot root after merge: %q", conf.NextSnapshotRoot)
	}
}

func TestResumeConfigCanOverrideFutureSnapshotRootAfterMerge(t *testing.T) {
	conf := &SnapshotConfig{Labels: map[string]string{}}
	mergeLabels(&snapshots.Info{Labels: map[string]string{
		common.SnapshotLabelSnapshotDir: "conch/snapshot/from-parent",
	}}, conf)
	conf.NextSnapshotRoot = nextSnapshotRootDir("sbx-3")

	if conf.RootDir != "conch/snapshot/from-parent" {
		t.Fatalf("unexpected restore input root dir: %q", conf.RootDir)
	}
	if conf.NextSnapshotRoot != "conch/snapshot/sbx-3" {
		t.Fatalf("unexpected future snapshot root dir: %q", conf.NextSnapshotRoot)
	}
}
