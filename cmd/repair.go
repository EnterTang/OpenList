package cmd

import "github.com/spf13/cobra"

var RepairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Inspect and repair durable OpenList state",
}

func init() {
	RootCmd.AddCommand(RepairCmd)
}
