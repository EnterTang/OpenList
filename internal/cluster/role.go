package cluster

import "strings"

type Role string

const (
	RoleStandalone  Role = "standalone"
	RoleCoordinator Role = "coordinator"
	RoleWorker      Role = "worker"
	RoleHybrid      Role = "hybrid"
)

func ParseRole(value string) Role {
	switch Role(strings.ToLower(strings.TrimSpace(value))) {
	case RoleCoordinator:
		return RoleCoordinator
	case RoleWorker:
		return RoleWorker
	case RoleHybrid:
		return RoleHybrid
	default:
		return RoleStandalone
	}
}

func (r Role) RunsCoordinator() bool {
	return r == RoleCoordinator || r == RoleHybrid
}

func (r Role) RunsWorker() bool {
	return r == RoleWorker || r == RoleHybrid
}

func (r Role) RunsStandaloneSchedulers() bool {
	return r == RoleStandalone
}
