package db

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/daangn/dynamoutil/pkg/config"
	"github.com/rs/zerolog/log"
)

// DynamoDBConfig represents required parameters to open
// a remote session with dynamodb table.
type DynamoDBConfig struct {
	Region    string
	TableName string

	// Endpoint required for local.
	Endpoint string
}

// SyncDynamoDBConfig includes origin and target dynamodb configs
type SyncDynamoDBConfig struct {
	Origin *DynamoDBConfig
	Target *DynamoDBConfig
}

func batchWrite(db *dynamodb.DynamoDB, r map[string][]*dynamodb.WriteRequest) {
	o, err := db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: r,
	})
	if err != nil {
		log.Fatal().Err(err).Interface("items", r).Msg("Failed to batch write items")
	}

	for _, v := range o.UnprocessedItems {
		if len(v) > 0 {
			batchWrite(db, o.UnprocessedItems)
		}
	}
}

func new(cfg *config.DynamoDBConfig) (*dynamodb.DynamoDB, error) {
	conf := &aws.Config{}
	conf.Region = &cfg.Region

	if cfg.Endpoint != "" {
		conf.Endpoint = aws.String(cfg.Endpoint)
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		cred := credentials.NewCredentials(&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     cfg.AccessKeyID,
				SecretAccessKey: cfg.SecretAccessKey,
			},
		})
		conf.WithCredentials(cred)
	}

	ss, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}
	return dynamodb.New(ss), nil
}
