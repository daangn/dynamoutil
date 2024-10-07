package cmd

import (
	"github.com/daangn/dynamoutil/pkg/config"
	"github.com/daangn/dynamoutil/pkg/db"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// renameCmd represents the rename command
var renameCmd = &cobra.Command{
	Use:   "rename",
	Short: "Rename attributes in the DynamoDB table as defined in the configuration",
	Long: `This command allows renaming of attributes in DynamoDB items based on the 
    before-after pairs defined in the configuration file. This requires read and write capacity 
    on the DynamoDB table.`,
	Args: cobra.RangeArgs(0, 1),
	PreRun: func(cmd *cobra.Command, args []string) {
		config.MustReadCfgFile()
	},
	Run: func(cmd *cobra.Command, args []string) {
		service := defaultService
		if len(args) == 1 {
			service = args[0]
		}

		for _, cfg := range config.MustBind().Rename {
			if cfg.Service == service {
				if err := db.Rename(cfg); err != nil {
					log.Fatal().Msgf("failed to rename attributes: %s", err)
				}
				return
			}
		}
		log.Error().Msgf("'%s' is not a valid service", service)
	},
}

func init() {
	rootCmd.AddCommand(renameCmd)
}
