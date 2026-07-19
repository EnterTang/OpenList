package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/OpenListTeam/OpenList/v4/internal/repair"
	"github.com/glebarez/sqlite"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	repairSourceReadEOFDBPath       string
	repairSourceReadEOFTablePrefix  string
	repairSourceReadEOFApply        bool
	repairSourceReadEOFLimit        int
	repairSourceReadEOFAttemptLimit uint64
)

var RepairSourceReadEOFCmd = &cobra.Command{
	Use:   "source-read-eof",
	Short: "Queue failed Pan123 source reads for bounded retry",
	RunE: func(cmd *cobra.Command, args []string) error {
		if repairSourceReadEOFDBPath == "" {
			return fmt.Errorf("--db is required")
		}
		databasePath, err := filepath.Abs(repairSourceReadEOFDBPath)
		if err != nil {
			return err
		}
		database, err := gorm.Open(sqlite.Open(databasePath), &gorm.Config{
			NamingStrategy: schema.NamingStrategy{TablePrefix: repairSourceReadEOFTablePrefix},
			Logger:         logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			return err
		}
		sqlDB, err := database.DB()
		if err != nil {
			return err
		}
		defer sqlDB.Close()
		report, err := repair.RepairSourceReadEOF(cmd.Context(), database, repair.SourceReadEOFOptions{
			Apply: repairSourceReadEOFApply, Limit: repairSourceReadEOFLimit, AttemptLimit: repairSourceReadEOFAttemptLimit,
		})
		if err != nil {
			return err
		}
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
		if !repairSourceReadEOFApply {
			fmt.Fprintln(cmd.OutOrStdout(), "dry-run only; rerun with --apply to commit these changes")
		}
		return nil
	},
}

func init() {
	RepairSourceReadEOFCmd.Flags().StringVar(&repairSourceReadEOFDBPath, "db", "", "path to the SQLite data.db file")
	RepairSourceReadEOFCmd.Flags().StringVar(&repairSourceReadEOFTablePrefix, "table-prefix", "x_", "database table prefix")
	RepairSourceReadEOFCmd.Flags().BoolVar(&repairSourceReadEOFApply, "apply", false, "commit repairs (default is a rolled-back dry-run)")
	RepairSourceReadEOFCmd.Flags().IntVar(&repairSourceReadEOFLimit, "limit", 100, "maximum number of candidate jobs to inspect")
	RepairSourceReadEOFCmd.Flags().Uint64Var(&repairSourceReadEOFAttemptLimit, "attempt-limit", 3, "maximum automatic attempt generation to allow")
	RepairCmd.AddCommand(RepairSourceReadEOFCmd)
}
