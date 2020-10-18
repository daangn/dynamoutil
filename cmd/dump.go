package cmd

import (
	"github.com/daangn/dynamoutil/pkg/config"
	"github.com/daangn/dynamoutil/pkg/db"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump items from the remote table",
	Long: `This command is working based on DynamoDB's BatchGetItems and BatchWriteItems.
	This requires read and write capacity of DynamoDB. If you turn on the flag 'on demand'
	on DynamoDB, please check before executing this command to prevent from billing costs by AWS.`,
	Args: cobra.RangeArgs(0, 1),
	PreRun: func(cmd *cobra.Command, args []string) {
		config.MustReadCfgFile()
	},
	Run: func(cmd *cobra.Command, args []string) {
		service := defaultService
		if len(args) == 1 {
			service = args[0]
		}

		for _, cfg := range config.MustBind().Dump {
			if cfg.Service == service {
				if err := db.Dump(cfg); err != nil {
					log.Fatal().Msgf("failed to sync: %s", err)
				}
				return
			}
		}
		log.Error().Msgf("'%s' is not a valid service", service)
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)
}
