package db

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/daangn/dynamoutil/pkg/config"
	"github.com/rs/zerolog/log"

	. "github.com/logrusorgru/aurora"
)

// Copy copy dynamodb items from origin to target table.
// This performs BatchGetItems from origin dynamodb table, and
// BatchPutItems to target dynamodb table.
func Copy(cfg *config.DynamoDBCopyConfig) error {
	fmt.Println(
		Bold(Green("Origin")),
		BrightBlue("region: ").String()+cfg.Origin.Region+" ",
		BrightBlue("table: ").String()+cfg.Origin.TableName+" ",
		BrightBlue("endpoint: ").String()+cfg.Origin.Endpoint,
	)
	fmt.Println(
		Bold(Green("Target")),
		BrightBlue("region: ").String()+cfg.Target.Region+" ",
		BrightBlue("table: ").String()+cfg.Target.TableName+" ",
		BrightBlue("endpoint: ").String()+cfg.Target.Endpoint,
	)

	fmt.Printf("\nAre you sure about copying all items from %s? [Y/n] ", BrightBlue(cfg.Origin.TableName))
	yn, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.Trim(yn, "\n") != "Y" {
		fmt.Println(Green("Goodbye👋"))
		return nil
	}
	fmt.Print("\n")

	originDB, err := new(cfg.Origin)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to origin database. Check .dynamoutil.yaml or origin database status")
	}
	targetDB, err := new(cfg.Target)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to target database. Check .dynamoutil.yaml or target database status")
	}

	oo, err := originDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &cfg.Origin.TableName,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Origin table does not exist")
	}

	_, err = targetDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &cfg.Target.TableName,
	})
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundException") {
			fmt.Printf("\nTable does not exist on <%s>.\nDo you want to create %s table at target endpoint?[Y/n] ",
				BrightBlue(fmt.Sprintf("%s %s %s", cfg.Target.Region, cfg.Target.TableName, cfg.Target.Endpoint)),
				BrightBlue(cfg.Target.TableName),
			)
			yn, _ := bufio.NewReader(os.Stdin).ReadString('\n')
			if strings.Trim(yn, "\n") != "Y" {
				fmt.Println("Goodbye~ 👋")
				return nil
			}

			cti := &dynamodb.CreateTableInput{
				KeySchema:            oo.Table.KeySchema,
				AttributeDefinitions: oo.Table.AttributeDefinitions,
				BillingMode:          oo.Table.BillingModeSummary.BillingMode,
				TableName:            &cfg.Target.TableName,
			}

			wcu := oo.Table.ProvisionedThroughput.WriteCapacityUnits
			if *wcu < 1 {
				wcu = aws.Int64(1)
			}

			rcu := oo.Table.ProvisionedThroughput.ReadCapacityUnits
			if *rcu < 1 {
				rcu = aws.Int64(1)
			}
			cti.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  rcu,
				WriteCapacityUnits: wcu,
			}

			if len(oo.Table.GlobalSecondaryIndexes) > 0 {
				var gsi []*dynamodb.GlobalSecondaryIndex
				for _, idx := range oo.Table.GlobalSecondaryIndexes {
					gsi = append(gsi, &dynamodb.GlobalSecondaryIndex{
						IndexName:             idx.IndexName,
						KeySchema:             idx.KeySchema,
						Projection:            idx.Projection,
						ProvisionedThroughput: cti.ProvisionedThroughput,
					})
				}
				cti.GlobalSecondaryIndexes = gsi
			}

			if len(oo.Table.LocalSecondaryIndexes) > 0 {
				var lsi []*dynamodb.LocalSecondaryIndex
				for _, idx := range oo.Table.LocalSecondaryIndexes {
					lsi = append(lsi, &dynamodb.LocalSecondaryIndex{
						IndexName:  idx.IndexName,
						KeySchema:  idx.KeySchema,
						Projection: idx.Projection,
					})
				}
				cti.LocalSecondaryIndexes = lsi
			}

			_, err := targetDB.CreateTable(cti)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create target dynamodb")
			}
		} else {
			log.Fatal().Err(err).Msg("Failed to describe target dynamodb table")
		}
	}

	fmt.Println()
	var lastKey map[string]*dynamodb.AttributeValue

	wg := sync.WaitGroup{}
	now := time.Now()
	var ops int32
	var readOps int32
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("\r\tTime spent: %.1f. Read %d items, Writes %d items. %.2f items/s", time.Since(now).Seconds(), Blue(readOps), Blue(ops), Blue(float64(ops)/(time.Since(now).Seconds())))
		}
	}()

	for {
		o, err := originDB.Scan(&dynamodb.ScanInput{
			TableName:         &cfg.Origin.TableName,
			Limit:             aws.Int64(2500),
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to scan origin dynamodb")
		}
		atomic.AddInt32(&readOps, int32(len(o.Items)))

		var (
			chunks [][]*dynamodb.WriteRequest
			wrs    []*dynamodb.WriteRequest
		)
		cnt := len(o.Items)
		for i, item := range o.Items {
			wrs = append(wrs, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: item,
				},
			})
			if (i+1)%25 == 0 || i == cnt-1 {
				chunks = append(chunks, wrs)
				wrs = []*dynamodb.WriteRequest{}
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, ch := range chunks {
				batchWrite(targetDB, map[string][]*dynamodb.WriteRequest{
					cfg.Target.TableName: ch,
				})
				atomic.AddInt32(&ops, int32(len(ch)))
			}

		}()

		if o.LastEvaluatedKey != nil {
			lastKey = o.LastEvaluatedKey
			continue
		}

		break
	}
	wg.Wait()
	since := time.Since(now)
	time.Sleep(time.Millisecond * 110)

	fmt.Print("\n\n")
	fmt.Printf("Copied %d items of %s table.\nExecution Time: %.2f seconds\nAvg: %.2f ops/s\n",
		Green(ops),
		BrightBlue(cfg.Origin.TableName),
		Green(since.Seconds()),
		Green(float64(ops)/since.Seconds()),
	)
	return nil
}
