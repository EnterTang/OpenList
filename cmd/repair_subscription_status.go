package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/repair"
	"github.com/glebarez/sqlite"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	repairSubscriptionDBPath            string
	repairSubscriptionTablePrefix       string
	repairSubscriptionApply             bool
	repairSubscriptionReconcileUnknown  bool
	repairSubscriptionDeclareIdempotent bool
)

var RepairSubscriptionStatusCmd = &cobra.Command{
	Use:   "subscription-status",
	Short: "Converge stale subscription, cluster stage, and target notification states",
	RunE: func(cmd *cobra.Command, args []string) error {
		if repairSubscriptionDBPath == "" {
			return fmt.Errorf("--db is required")
		}
		databasePath, err := filepath.Abs(repairSubscriptionDBPath)
		if err != nil {
			return err
		}
		database, err := gorm.Open(sqlite.Open(databasePath), &gorm.Config{
			NamingStrategy: schema.NamingStrategy{TablePrefix: repairSubscriptionTablePrefix},
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
		report, err := repair.RepairSubscriptionStatuses(cmd.Context(), database, repair.SubscriptionStatusOptions{
			Apply: repairSubscriptionApply, ReconcileUnknown: repairSubscriptionReconcileUnknown,
			DeclareTargetIdempotent: repairSubscriptionDeclareIdempotent,
			Timeout:                 15 * time.Second, Limit: 100,
		})
		if err != nil {
			return err
		}
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
		if !repairSubscriptionApply {
			fmt.Fprintln(cmd.OutOrStdout(), "dry-run only; rerun with --apply to commit these changes")
		}
		return nil
	},
}

func init() {
	RepairSubscriptionStatusCmd.Flags().StringVar(&repairSubscriptionDBPath, "db", "", "path to the SQLite data.db file")
	RepairSubscriptionStatusCmd.Flags().StringVar(&repairSubscriptionTablePrefix, "table-prefix", "x_", "database table prefix")
	RepairSubscriptionStatusCmd.Flags().BoolVar(&repairSubscriptionApply, "apply", false, "commit repairs (default is a rolled-back dry-run)")
	RepairSubscriptionStatusCmd.Flags().BoolVar(&repairSubscriptionReconcileUnknown, "reconcile-unknown", true, "lookup unknown create jobs in the target service")
	RepairSubscriptionStatusCmd.Flags().BoolVar(&repairSubscriptionDeclareIdempotent, "declare-target-idempotent", false, "declare the configured target safe for automatic retries")
	RepairCmd.AddCommand(RepairSubscriptionStatusCmd)
}
