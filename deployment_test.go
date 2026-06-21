package tinysql

import (
	"context"
	"path/filepath"
	"testing"
)

func TestParseDeploymentMode(t *testing.T) {
	tests := []struct {
		input string
		want  DeploymentMode
		err   bool
	}{
		{"", DeploymentPackage, false},
		{"package", DeploymentPackage, false},
		{"sqlite", DeploymentEmbedded, false},
		{"server", DeploymentServer, false},
		{"enterprise", DeploymentEnterprise, false},
		{"unknown", DeploymentPackage, true},
	}

	for _, tc := range tests {
		got, err := ParseDeploymentMode(tc.input)
		if (err != nil) != tc.err {
			t.Fatalf("ParseDeploymentMode(%q) error = %v, wantErr %v", tc.input, err, tc.err)
		}
		if got != tc.want {
			t.Fatalf("ParseDeploymentMode(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestOpenPackageInstance(t *testing.T) {
	inst := OpenPackage("tenant1")
	defer inst.Close()

	if inst.Mode != DeploymentPackage {
		t.Fatalf("mode = %v, want %v", inst.Mode, DeploymentPackage)
	}
	if inst.Tenant != "tenant1" {
		t.Fatalf("tenant = %q", inst.Tenant)
	}

	if _, err := inst.ExecuteSQL(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("ExecuteSQL CREATE TABLE failed: %v", err)
	}
	if _, err := inst.ExecuteSQL(context.Background(), "INSERT INTO users VALUES (1, 'Ada')"); err != nil {
		t.Fatalf("ExecuteSQL INSERT failed: %v", err)
	}
	rs, err := inst.ExecuteSQL(context.Background(), "SELECT name FROM users")
	if err != nil {
		t.Fatalf("ExecuteSQL SELECT failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Ada" {
		t.Fatalf("unexpected rows: %#v", rs.Rows)
	}
}

func TestOpenEmbeddedInstance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "embedded.gob")
	inst, err := OpenEmbedded(path, "")
	if err != nil {
		t.Fatalf("OpenEmbedded failed: %v", err)
	}
	defer inst.Close()

	if inst.Mode != DeploymentEmbedded {
		t.Fatalf("mode = %v, want %v", inst.Mode, DeploymentEmbedded)
	}
	if inst.DB.StorageMode() != ModeWAL {
		t.Fatalf("storage mode = %v, want %v", inst.DB.StorageMode(), ModeWAL)
	}
}

func TestOpenEnterpriseRequiresDurableStorage(t *testing.T) {
	if _, err := OpenEnterprise(StorageConfig{}, ""); err == nil {
		t.Fatal("expected enterprise mode without path to fail")
	}
	if _, err := OpenEnterprise(StorageConfig{Mode: ModeMemory, Path: filepath.Join(t.TempDir(), "db.gob")}, ""); err == nil {
		t.Fatal("expected enterprise memory mode to fail")
	}
}

func TestOpenEnterpriseStartsScheduler(t *testing.T) {
	dir := t.TempDir()
	inst, err := OpenEnterprise(StorageConfig{Mode: ModeDisk, Path: dir}, "")
	if err != nil {
		t.Fatalf("OpenEnterprise failed: %v", err)
	}
	defer inst.Close()

	if inst.Mode != DeploymentEnterprise {
		t.Fatalf("mode = %v, want %v", inst.Mode, DeploymentEnterprise)
	}
	if inst.DB.JobScheduler() == nil {
		t.Fatal("expected enterprise profile to start job scheduler")
	}
}

func TestInstanceStopStartRestartHealth(t *testing.T) {
	dir := t.TempDir()
	inst, err := OpenEnterprise(StorageConfig{Mode: ModeDisk, Path: dir}, "main")
	if err != nil {
		t.Fatalf("OpenEnterprise failed: %v", err)
	}
	defer inst.Close()

	if !inst.Health().OK {
		t.Fatalf("initial health = %#v", inst.Health())
	}
	if _, err := inst.ExecuteSQL(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if _, err := inst.ExecuteSQL(context.Background(), "INSERT INTO users VALUES (1, 'Ada')"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if err := inst.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	stopped := inst.Health()
	if stopped.OK || !stopped.Closed || stopped.LastCloseAt.IsZero() {
		t.Fatalf("stopped health mismatch: %#v", stopped)
	}

	if err := inst.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !inst.Health().OK || inst.DB.JobScheduler() == nil {
		t.Fatalf("started health mismatch: %#v", inst.Health())
	}
	rs, err := inst.ExecuteSQL(context.Background(), "SELECT name FROM users")
	if err != nil {
		t.Fatalf("SELECT after start failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Ada" {
		t.Fatalf("unexpected rows after start: %#v", rs.Rows)
	}

	if err := inst.Restart(); err != nil {
		t.Fatalf("Restart failed: %v", err)
	}
	if !inst.Health().OK || inst.DB.JobScheduler() == nil {
		t.Fatalf("restarted health mismatch: %#v", inst.Health())
	}
}
