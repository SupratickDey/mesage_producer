package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

// Config holds all application configuration
type Config struct {
	Producer ProducerConfig `yaml:"producer"`
	Output   OutputConfig   `yaml:"output"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Data     DataConfig     `yaml:"data"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

// ProducerConfig holds producer-specific settings
type ProducerConfig struct {
	MessageCount int `yaml:"message_count"`
	Workers      int `yaml:"workers"`
	BufferSize   int `yaml:"buffer_size"`
}

// OutputConfig holds output-related configuration
type OutputConfig struct {
	Format    string        `yaml:"format"`
	Directory string        `yaml:"directory"`
	CSV       CSVConfig     `yaml:"csv"`
	Parquet   ParquetConfig `yaml:"parquet"`
}

// CSVConfig holds CSV-specific settings
type CSVConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Filename   string `yaml:"filename"`
	BufferSize int    `yaml:"buffer_size"`
}

// ParquetConfig holds Parquet-specific settings
type ParquetConfig struct {
	Enabled      bool   `yaml:"enabled"`
	Filename     string `yaml:"filename"`
	RowGroupSize int    `yaml:"row_group_size"`
	Compression  string `yaml:"compression"`
}

// KafkaConfig holds Kafka-related configuration
type KafkaConfig struct {
	Enabled        bool     `yaml:"enabled"`
	Brokers        []string `yaml:"brokers"`
	Topic          string   `yaml:"topic"`
	Compression    string   `yaml:"compression"`
	BatchSize      int      `yaml:"batch_size"`
	FlushFrequency int      `yaml:"flush_frequency"`
	Async          bool     `yaml:"async"`
}

// DataConfig holds paths to data files
type DataConfig struct {
	CurrencyRates  string `yaml:"currency_rates"`
	Agents         string `yaml:"agents"`
	GameCategories string `yaml:"game_categories"`
	Currencies     string `yaml:"currencies"`
}

// MetricsConfig holds metrics-related configuration
type MetricsConfig struct {
	Interval int  `yaml:"interval"`
	Detailed bool `yaml:"detailed"`
}

// Load reads and parses the configuration file
func Load(path string) (*Config, error) {
	// Try to load .env file if it exists (non-fatal if missing)
	_ = godotenv.Load()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override with environment variables if present
	cfg.applyEnvOverrides()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// ApplyEnvOverrides is a public wrapper for applyEnvOverrides
func (c *Config) ApplyEnvOverrides() {
	c.applyEnvOverrides()
}

// applyEnvOverrides applies environment variable overrides to the configuration
func (c *Config) applyEnvOverrides() {
	// Producer config
	if v := os.Getenv("PRODUCER_MESSAGE_COUNT"); v != "" {
		if count, err := strconv.Atoi(v); err == nil {
			c.Producer.MessageCount = count
		}
	}
	if v := os.Getenv("PRODUCER_WORKERS"); v != "" {
		if workers, err := strconv.Atoi(v); err == nil {
			c.Producer.Workers = workers
		}
	}
	if v := os.Getenv("PRODUCER_BUFFER_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil {
			c.Producer.BufferSize = size
		}
	}

	// Output config
	if v := os.Getenv("OUTPUT_FORMAT"); v != "" {
		c.Output.Format = v
	}
	if v := os.Getenv("OUTPUT_DIRECTORY"); v != "" {
		c.Output.Directory = v
	}

	// CSV config
	if v := os.Getenv("CSV_ENABLED"); v != "" {
		c.Output.CSV.Enabled = v == "true"
	}
	if v := os.Getenv("CSV_FILENAME"); v != "" {
		c.Output.CSV.Filename = v
	}
	if v := os.Getenv("CSV_BUFFER_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil {
			c.Output.CSV.BufferSize = size
		}
	}

	// Parquet config
	if v := os.Getenv("PARQUET_ENABLED"); v != "" {
		c.Output.Parquet.Enabled = v == "true"
	}
	if v := os.Getenv("PARQUET_FILENAME"); v != "" {
		c.Output.Parquet.Filename = v
	}
	if v := os.Getenv("PARQUET_ROW_GROUP_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil {
			c.Output.Parquet.RowGroupSize = size
		}
	}
	if v := os.Getenv("PARQUET_COMPRESSION"); v != "" {
		c.Output.Parquet.Compression = v
	}

	// Kafka config
	if v := os.Getenv("KAFKA_ENABLED"); v != "" {
		c.Kafka.Enabled = v == "true"
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		c.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_TOPIC"); v != "" {
		c.Kafka.Topic = v
	}
	if v := os.Getenv("KAFKA_COMPRESSION"); v != "" {
		c.Kafka.Compression = v
	}
	if v := os.Getenv("KAFKA_BATCH_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil {
			c.Kafka.BatchSize = size
		}
	}
	if v := os.Getenv("KAFKA_FLUSH_FREQUENCY"); v != "" {
		if freq, err := strconv.Atoi(v); err == nil {
			c.Kafka.FlushFrequency = freq
		}
	}
	if v := os.Getenv("KAFKA_ASYNC"); v != "" {
		c.Kafka.Async = v == "true"
	}

	// Data config
	if v := os.Getenv("DATA_CURRENCY_RATES"); v != "" {
		c.Data.CurrencyRates = v
	}
	if v := os.Getenv("DATA_AGENTS"); v != "" {
		c.Data.Agents = v
	}
	if v := os.Getenv("DATA_GAME_CATEGORIES"); v != "" {
		c.Data.GameCategories = v
	}
	if v := os.Getenv("DATA_CURRENCIES"); v != "" {
		c.Data.Currencies = v
	}

	// Metrics config
	if v := os.Getenv("METRICS_INTERVAL"); v != "" {
		if interval, err := strconv.Atoi(v); err == nil {
			c.Metrics.Interval = interval
		}
	}
	if v := os.Getenv("METRICS_DETAILED"); v != "" {
		c.Metrics.Detailed = v == "true"
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Producer.MessageCount < 0 {
		return fmt.Errorf("message_count must be non-negative (0 for continuous mode)")
	}

	if c.Producer.Workers <= 0 {
		return fmt.Errorf("workers must be positive")
	}

	if c.Producer.BufferSize <= 0 {
		return fmt.Errorf("buffer_size must be positive")
	}

	if c.Output.Format != "csv" && c.Output.Format != "parquet" && c.Output.Format != "both" {
		return fmt.Errorf("output format must be 'csv', 'parquet', or 'both'")
	}

	if c.Kafka.Enabled {
		if len(c.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka brokers cannot be empty when kafka is enabled")
		}
		if c.Kafka.Topic == "" {
			return fmt.Errorf("kafka topic cannot be empty when kafka is enabled")
		}
	}

	return nil
}
