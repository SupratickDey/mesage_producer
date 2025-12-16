package config

import (
	"fmt"
	"os"

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
	Filename   string `yaml:"filename"`
	BufferSize int    `yaml:"buffer_size"`
}

// ParquetConfig holds Parquet-specific settings
type ParquetConfig struct {
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
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
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
