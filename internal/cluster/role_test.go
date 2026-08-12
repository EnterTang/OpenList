package cluster

import "testing"

func TestParseRole(t *testing.T) {
	tests := []struct {
		input string
		want  Role
	}{
		{"standalone", RoleStandalone},
		{"COORDINATOR", RoleCoordinator},
		{" worker ", RoleWorker},
		{"hybrid", RoleHybrid},
		{"unknown", RoleStandalone},
	}
	for _, tt := range tests {
		if got := ParseRole(tt.input); got != tt.want {
			t.Fatalf("ParseRole(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestRoleCapabilities(t *testing.T) {
	if !RoleCoordinator.RunsCoordinator() || RoleCoordinator.RunsWorker() {
		t.Fatal("coordinator role capabilities are invalid")
	}
	if !RoleWorker.RunsWorker() || RoleWorker.RunsCoordinator() {
		t.Fatal("worker role capabilities are invalid")
	}
	if !RoleHybrid.RunsCoordinator() || !RoleHybrid.RunsWorker() {
		t.Fatal("hybrid must run both coordinator and worker")
	}
	if !RoleStandalone.RunsStandaloneSchedulers() {
		t.Fatal("standalone must preserve local schedulers")
	}
}
