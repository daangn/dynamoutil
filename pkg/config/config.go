package config

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	. "github.com/logrusorgru/aurora"
)

// Config represents a global configuration
type Config struct {
	Copy []*DynamoDBCopyConfig `mapstructure:"copy"`
	Dump []*DynamoDBDumpConfig `mapstructure:"dump"`
}

// Output represents a file extension
type Output string

// Output constants
const (
	OutputJSON    Output = "json"
	OutputJSONRaw Output = "jsonRaw"
)

// DefaultOutput represents the default output
var DefaultOutput = OutputJSONRaw

// DumpPrefix returns a prefix string for dump
func (o Output) DumpPrefix() []byte {
	switch o {
	case OutputJSON:
		return []byte("[")
	default:
		return []byte("")
	}
}

// DumpDelimiter returns a delimiter string for dump
func (o Output) DumpDelimiter() []byte {
	switch o {
	case OutputJSON:
		return []byte(",")
	default:
		return []byte("\n")
	}
}

// DumpSuffix returns a suffix string for dump
func (o Output) DumpSuffix() []byte {
	switch o {
	case OutputJSON:
		return []byte("]")
	default:
		return []byte("")
	}
}

// DynamoDBCopyConfig maps origin and target configs for DynamoDB
type DynamoDBCopyConfig struct {
	Service string          `mapstructure:"service"`
	Origin  *DynamoDBConfig `mapstructure:"origin"`
	Target  *DynamoDBConfig `mapstructure:"target"`
}

// DynamoDBDumpConfig maps dump configs for DynamoDB
type DynamoDBDumpConfig struct {
	DynamoDB DynamoDBConfig `mapstructure:"db"`
	Service  string         `mapstructure:"service"`
	FileName string         `mapstructure:"filename"`
	Output   Output         `mapstructure:"output"`
}

// DynamoDBConfig represents connection info for a specific table
type DynamoDBConfig struct {
	Region    string `mapstructure:"region"`
	TableName string `mapstructure:"table"`
	// Required for DynamoDB local
	Endpoint        string `mapstructure:"endpoint"`
	AccessKeyID     string `mapstructure:"accessKeyID"`
	SecretAccessKey string `mapstructure:"secretAccessKey"`
}

// MustBind binds the read configurations to Config struct
func MustBind() *Config {
	var cfg Config
	// NOTE: viper uses "mapstructure" tags instead of "yaml" tags
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatal().Err(err)
	}
	return &cfg
}

// BindCopyConfigByKey binds the read configurations to Copy Config struct
func BindCopyConfigByKey(key string) (*Config, error) {
	var cfg Config
	// NOTE: viper uses "mapstructure" tags instead of "yaml" tags
	if err := viper.UnmarshalKey(key, &cfg.Copy); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// MustReadCfgFile reads the config file stated with or without given config file location
func MustReadCfgFile() {
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal().Err(err).Msgf("couldn't read the config file: %s", viper.ConfigFileUsed())
	}
	fmt.Println(Blue("Config file:" + viper.ConfigFileUsed() + "\n"))
}
