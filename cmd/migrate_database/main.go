package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/glebarez/sqlite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func main() {
	var (
		sourcePath = flag.String("source", "", "source SQLite database path")
		targetDSN  = flag.String("target", "", "target PostgreSQL DSN")
		prefix     = flag.String("prefix", "x_", "GORM table prefix")
		batchSize  = flag.Int("batch-size", 500, "rows per target insert batch")
		sampleSize = flag.Int("sample-size", 100, "rows per table used for validation hash")
		tables     = flag.String("tables", "", "comma-separated table names; defaults to all tables")
		dryRun     = flag.Bool("dry-run", false, "inspect source and target connectivity without writing")
		validate   = flag.Bool("validate-only", false, "validate an existing source/target pair without copying")
	)
	flag.Parse()
	if strings.TrimSpace(*sourcePath) == "" || strings.TrimSpace(*targetDSN) == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "-source and -target are required")
		os.Exit(2)
	}
	if *dryRun && *validate {
		fmt.Fprintln(os.Stderr, "-dry-run and -validate-only cannot be combined")
		os.Exit(2)
	}

	source, err := gorm.Open(sqlite.Open(*sourcePath), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: *prefix},
	})
	if err != nil {
		fatalf("open source SQLite database: %v", err)
	}
	target, err := gorm.Open(postgres.Open(*targetDSN), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: *prefix},
	})
	if err != nil {
		fatalf("open target PostgreSQL database: %v", err)
	}
	defer closeDatabase(source)
	defer closeDatabase(target)

	tableNames := make([]string, 0)
	if strings.TrimSpace(*tables) != "" {
		for _, table := range strings.Split(*tables, ",") {
			if table = strings.TrimSpace(table); table != "" {
				tableNames = append(tableNames, table)
			}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	report, err := db.MigrateSQLiteToPostgres(ctx, source, target, db.MigrationOptions{
		TablePrefix:  *prefix,
		TableNames:   tableNames,
		BatchSize:    *batchSize,
		SampleSize:   *sampleSize,
		DryRun:       *dryRun,
		ValidateOnly: *validate,
	})
	if err != nil {
		fatalf("database migration failed: %v", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		fatalf("write migration report: %v", err)
	}
}

func closeDatabase(database *gorm.DB) {
	sqlDB, err := database.DB()
	if err == nil {
		_ = sqlDB.Close()
	}
}

func fatalf(format string, args ...any) {
	var err error
	if len(args) == 1 {
		if candidate, ok := args[0].(error); ok {
			err = candidate
		}
	}
	if err != nil && errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "migration canceled")
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
	os.Exit(1)
}
