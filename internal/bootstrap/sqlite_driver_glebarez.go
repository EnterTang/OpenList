//go:build !sqlite_cgo_compat && !(linux && (mips || mips64 || mips64le || mipsle || loong64)) && !(windows && 386)

package bootstrap

import (
	"net/url"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func openSQLite(target string) gorm.Dialector {
	pragmas := url.Values{}
	pragmas.Add("_pragma", "journal_mode(WAL)")
	pragmas.Add("_pragma", "busy_timeout(5000)")
	pragmas.Add("_pragma", "auto_vacuum(incremental)")
	pragmas.Set("_txlock", "immediate")
	return sqlite.Open(sqliteDSN(target, pragmas))
}
