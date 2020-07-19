/*
Copyright © 2020 Bien <novemberde1@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/daangn/dynamoutil/pkg/config"
	"github.com/daangn/dynamoutil/pkg/db"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

const defaultService = "default"

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy items from the origin table, and import on the target table",
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

		for _, cfg := range config.MustBind().Copy {
			if cfg.Service == service {
				if err := db.Copy(cfg); err != nil {
					log.Fatal().Msgf("failed to sync: %s", err)
				}
				return
			}
		}
		log.Error().Msgf("'%s' is not a valid service", service)
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)
}
