package db

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/daangn/dynamoutil/pkg/config"
	"github.com/rs/zerolog/log"

	. "github.com/logrusorgru/aurora"
)

// Dump make a file
func Dump(cfg *config.DynamoDBDumpConfig) error {
	fmt.Println(
		Bold(Green("service: ").String()+cfg.Service+" "),
		BrightBlue("region: ").String()+cfg.DynamoDB.Region+" ",
		BrightBlue("table: ").String()+cfg.DynamoDB.TableName+" ",
		BrightBlue("endpoint: ").String()+cfg.DynamoDB.Endpoint+" ",
		BrightBlue("output: ").String()+string(cfg.Output)+" ",
	)

	fmt.Printf("\nAre you sure about dumping all items from %s? [Y/n] ", BrightBlue(cfg.DynamoDB.TableName))
	yn, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.Trim(yn, "\n") != "Y" {
		fmt.Println(Green("Goodbye👋"))
		return nil
	}
	fmt.Print("\n")

	remoteDB, err := new(&cfg.DynamoDB)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to origin database. Check .dynamoutil.yaml or origin database status")
	}

	file, err := os.Create(cfg.FileName)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open file")
	}
	defer file.Close()

	if cfg.Output == "" {
		cfg.Output = config.DefaultOutput
	}

	now := time.Now()
	var ops int32
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("\r    Writes %d items. %.2f items/s", Blue(ops), Blue(float64(ops)/(time.Since(now).Seconds())))
		}
	}()

	file.Write(cfg.Output.DumpPrefix())
	var lastKey map[string]*dynamodb.AttributeValue
	for {
		o, err := remoteDB.Scan(&dynamodb.ScanInput{
			TableName:         &cfg.DynamoDB.TableName,
			Limit:             aws.Int64(10000),
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to scan origin dynamodb")
		}

		max := len(o.Items) - 1
		for i, item := range o.Items {
			b, err := json.Marshal(item)
			if err != nil {
				log.Err(err).Send()
				continue
			}

			var jsonItem map[string]interface{}
			if err := json.Unmarshal(b, &jsonItem); err != nil {
				log.Err(err).Send()
				continue
			}

			marshaled, err := marshalDynamo(jsonItem)
			if err != nil {
				log.Err(err).Msg("failed to marshal dynamodb object")
				continue
			}

			b2, err := json.Marshal(marshaled)
			if err != nil {
				log.Err(err).Msg("failed to marshal dynamodb object to json")
				continue
			}

			file.Write(b2)
			if max != i {
				file.Write(cfg.Output.DumpDelimiter())
			}
			ops++
		}
		if o.LastEvaluatedKey != nil {
			lastKey = o.LastEvaluatedKey
			file.Write(cfg.Output.DumpDelimiter())
			continue
		}

		break
	}
	file.Write(cfg.Output.DumpSuffix())

	return nil
}
