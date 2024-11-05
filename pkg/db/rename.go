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

// RenameMetrics holds metrics for each rename operation.
type renameMetrics struct {
	Count    int32
	Duration time.Duration
}

// Rename reads before-after pairs from the YAML file and renames attributes in a DynamoDB table.
func Rename(cfg *config.DynamoDBRenameConfig) error {
	fmt.Println(
		Bold(Green("Target")),
		BrightBlue("region: ").String()+cfg.Target.Region+" ",
		BrightBlue("table: ").String()+cfg.Target.TableName+" ",
		BrightBlue("endpoint: ").String()+cfg.Target.Endpoint,
	)

	fmt.Printf("\nAre you sure about renaming attributes in %s? [Y/n] ", BrightBlue(cfg.Target.TableName))
	yn, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.Trim(yn, "\n") != "Y" {
		fmt.Println(Green("Goodbye👋"))
		return nil
	}
	fmt.Print("\n")

	targetDB, err := new(cfg.Target)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to target database. Check .dynamoutil.yaml or target database status")
	}

	oo, err := targetDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &cfg.Target.TableName,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Target table does not exist")
	}

	// Extract the primary key attributes from the KeySchema
	var partitionKey, sortKey string
	for _, keyElement := range oo.Table.KeySchema {
		if *keyElement.KeyType == "HASH" {
			partitionKey = *keyElement.AttributeName
		} else if *keyElement.KeyType == "RANGE" {
			sortKey = *keyElement.AttributeName
		}
	}
	fmt.Printf("Partition Key: %s, Sort Key: %s\n", partitionKey, sortKey)

	fmt.Println()
	var lastKey map[string]*dynamodb.AttributeValue

	// Metrics for each rename operation
	metrics := make(map[string]*renameMetrics)
	for _, rename := range cfg.Rename {
		metrics[fmt.Sprintf("%s -> %s", rename.Before, rename.After)] = &renameMetrics{}
	}

	wg := sync.WaitGroup{}
	now := time.Now()
	var ops int32
	var readOps int32

	// Display progress
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("\r\tTime spent: %.1f. Read %d items, Processed %d items. %.2f items/s", time.Since(now).Seconds(), Blue(readOps), Blue(ops), Blue(float64(ops)/(time.Since(now).Seconds())))
		}
	}()

	// Scan and process items
	for {
		o, err := targetDB.Scan(&dynamodb.ScanInput{
			TableName:         &cfg.Target.TableName,
			Limit:             aws.Int64(2500),
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to scan target dynamodb")
		}
		atomic.AddInt32(&readOps, int32(len(o.Items)))

		var (
			deleteChunks [][]*dynamodb.WriteRequest
			putChunks    [][]*dynamodb.WriteRequest
			deleteWrs    []*dynamodb.WriteRequest
			putWrs       []*dynamodb.WriteRequest
		)

		for _, item := range o.Items {
			// Track time spent on each rename operation
			itemStart := time.Now()
			renamed := false
			// Rename attributes based on the configuration
			for _, rename := range cfg.Rename {
				if val, exists := item[rename.Before]; exists {
					item[rename.After] = val
					delete(item, rename.Before)
					metricsKey := fmt.Sprintf("%s -> %s", rename.Before, rename.After)
					atomic.AddInt32(&metrics[metricsKey].Count, 1)
					metrics[metricsKey].Duration += time.Since(itemStart)
					renamed = true
				}
			}

			if !renamed {
				continue
			}

			atomic.AddInt32(&ops, 1)

			key := map[string]*dynamodb.AttributeValue{
				partitionKey: item[partitionKey],
				sortKey:      item[sortKey],
			}

			// Prepare DeleteRequest and PutRequest
			deleteRequest := &dynamodb.DeleteRequest{Key: key}
			putRequest := &dynamodb.PutRequest{Item: item}

			// Add the Delete request to deleteWrs
			deleteWrs = append(deleteWrs, &dynamodb.WriteRequest{
				DeleteRequest: deleteRequest,
			})

			// Add the Put request to putWrs
			putWrs = append(putWrs, &dynamodb.WriteRequest{
				PutRequest: putRequest,
			})

			// Batch in chunks of 25 requests as DynamoDB limits
			if len(deleteWrs) >= 25 {
				deleteChunks = append(deleteChunks, deleteWrs)
				deleteWrs = []*dynamodb.WriteRequest{}
			}
			if len(putWrs) >= 25 {
				putChunks = append(putChunks, putWrs)
				putWrs = []*dynamodb.WriteRequest{}
			}

			// Record time taken for renaming this item
			for _, rename := range cfg.Rename {
				metricsKey := fmt.Sprintf("%s -> %s", rename.Before, rename.After)
				metrics[metricsKey].Duration += time.Since(itemStart)
			}
		}

		// Add remaining requests to chunks
		if len(deleteWrs) > 0 {
			deleteChunks = append(deleteChunks, deleteWrs)
		}
		if len(putWrs) > 0 {
			putChunks = append(putChunks, putWrs)
		}

		// Process delete requests
		for _, chunk := range deleteChunks {
			wg.Add(1)
			go func(chunk []*dynamodb.WriteRequest) {
				defer wg.Done()
				batchWrite(targetDB, map[string][]*dynamodb.WriteRequest{
					cfg.Target.TableName: chunk,
				})
			}(chunk)
		}

		// Wait for all delete requests to complete before proceeding with put requests
		wg.Wait()

		// Process put requests
		for _, chunk := range putChunks {
			wg.Add(1)
			go func(chunk []*dynamodb.WriteRequest) {
				defer wg.Done()
				batchWrite(targetDB, map[string][]*dynamodb.WriteRequest{
					cfg.Target.TableName: chunk,
				})
			}(chunk)
		}
		wg.Wait()

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
	fmt.Printf("Renamed %d items of %s table.\nExecution Time: %.2f seconds\nAvg: %.2f ops/s\n",
		Green(ops),
		BrightBlue(cfg.Target.TableName),
		Green(since.Seconds()),
		Green(float64(ops)/since.Seconds()),
	)

	// Print metrics for each rename operation
	fmt.Println("\nDetailed Rename Metrics:")
	for key, metric := range metrics {
		if metric.Count == 0 {
			fmt.Printf("%s: No items changed\n", BrightBlue(key))
			continue
		}

		avgTime := metric.Duration.Seconds() / float64(metric.Count)
		fmt.Printf("%s: %d items changed, Total Time: %.2f seconds, Avg Time per item: %.4f seconds\n",
			BrightBlue(key),
			Green(metric.Count),
			Green(metric.Duration.Seconds()),
			Green(avgTime),
		)
	}

	return nil
}
