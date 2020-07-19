package copy

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
	"github.com/daangn/dynamoutil/pkg/db"
	. "github.com/logrusorgru/aurora"
	"github.com/rs/zerolog/log"
)

// Copy migrate dynamodb items from origin to target table.
// This performs BatchGetItems from origin dynamodb table, and
// BatchPutItems to target dynamodb table.
func Copy(c *config.CopyConfig) {
	fmt.Printf("%s  region:%s table:%s endpoint:%s \n", Green("ORIGIN"), Magenta(c.Origin.Region), Magenta(c.Origin.TableName), Magenta(c.Origin.Endpoint))
	fmt.Printf("%s  region:%s table:%s endpoint:%s \n", Green("TARGET"), Magenta(c.Target.Region), Magenta(c.Target.TableName), Magenta(c.Target.Endpoint))
	fmt.Printf("...\nAre you sure about copying all items from %s?[y/n] ", Magenta(c.Origin.TableName))
	txt1, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.Trim(string(txt1), "\n") != "y" {
		fmt.Println("Goodbye~ 👋")
		return
	}
	fmt.Print("\n")

	originDB, err := db.New(c.Origin)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to origin database. Check .dynamoutil.yaml or origin database status")
	}
	targetDB, err := db.New(c.Target)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to target database. Check .dynamoutil.yaml or target database status")
	}

	oo, err := originDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &c.Origin.TableName,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Origin table does not exist")
	}

	_, err = targetDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &c.Target.TableName,
	})
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundException") {
			fmt.Printf("Table does not exist on <%s %s %s>.\nDo you want to create %s table?[y/n] ",
				Magenta(c.Target.Region), Magenta(c.Target.TableName), Magenta(c.Target.Endpoint), Magenta(c.Target.TableName))
			txt, _ := bufio.NewReader(os.Stdin).ReadString('\n')
			if strings.Trim(string(txt), "\n") != "y" {
				fmt.Println("Goodbye~ 👋")
				return
			}

			cti := &dynamodb.CreateTableInput{
				KeySchema:            oo.Table.KeySchema,
				AttributeDefinitions: oo.Table.AttributeDefinitions,
				BillingMode:          oo.Table.BillingModeSummary.BillingMode,
				TableName:            &c.Target.TableName,
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

	var lastKey map[string]*dynamodb.AttributeValue

	wg := sync.WaitGroup{}
	now := time.Now()
	var ops int32
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("\r    Writes %s items(%.2f items/s)", Green(fmt.Sprintf("%d", ops)), float64(ops)/(time.Since(now).Seconds()))
		}
	}()

	for {
		o, err := originDB.Scan(&dynamodb.ScanInput{
			TableName:         &c.Origin.TableName,
			Limit:             aws.Int64(10000),
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to scan origin dynamodb")
		}

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
			if (i+1)%10 == 0 || i == cnt-1 {
				chunks = append(chunks, wrs)
				wrs = []*dynamodb.WriteRequest{}
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, ch := range chunks {
				targetDB.BatchWrite(map[string][]*dynamodb.WriteRequest{
					c.Target.TableName: ch,
				})
			}

			atomic.AddInt32(&ops, int32(len(o.Items)))
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
	fmt.Printf("Copied dynamodb %s items. Execution Time: %s seconds\n",
		Green(fmt.Sprintf("%d", ops)),
		Green(fmt.Sprintf("%.2f", since.Seconds())))
}
