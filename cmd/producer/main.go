package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/supratick/message_producer/internal/config"
	"github.com/supratick/message_producer/internal/generator"
	"github.com/supratick/message_producer/internal/metrics"
	"github.com/supratick/message_producer/internal/models"
	"github.com/supratick/message_producer/internal/writer"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	flag.Parse()

	// Initialize structured logging
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting message producer", "version", "1.0.0")

	// Check if config file exists
	var cfg *config.Config
	var err error
	
	if _, statErr := os.Stat(*configPath); os.IsNotExist(statErr) {
		// Config file doesn't exist, use defaults with environment overrides
		slog.Warn("Config file not found, using defaults with environment overrides", "config_path", *configPath)
		cfg = &config.Config{
			Producer: config.ProducerConfig{
				MessageCount: 0,
				Workers:      4,
				BufferSize:   10000,
			},
			Output: config.OutputConfig{
				Format:    "parquet",
				Directory: "/app/output",
				CSV: config.CSVConfig{
					Enabled:    false,
					Filename:   "transactions.csv",
					BufferSize: 10000,
				},
				Parquet: config.ParquetConfig{
					Enabled:      false,
					Filename:     "transactions.parquet",
					RowGroupSize: 50000,
					Compression:  "snappy",
				},
			},
			Kafka: config.KafkaConfig{
				Enabled:        false,
				Brokers:        []string{"localhost:9092"},
				Topic:          "transactions",
				Compression:    "snappy",
				BatchSize:      5000,
				FlushFrequency: 100,
				Async:          true,
			},
			Data: config.DataConfig{
				CurrencyRates:  "/app/data/currency_rates.json",
				Agents:         "/app/data/agents.json",
				GameCategories: "/app/data/game_categories.json",
				Currencies:     "/app/data/currencies.json",
			},
			Metrics: config.MetricsConfig{
				Interval: 5,
				Detailed: true,
			},
		}
		// Apply environment variable overrides
		cfg.ApplyEnvOverrides()
		
		// Validate the configuration
		if err := cfg.Validate(); err != nil {
			slog.Error("Invalid configuration", "error", err)
			os.Exit(1)
		}
	} else {
		// Load configuration from file
		cfg, err = config.Load(*configPath)
		if err != nil {
			slog.Error("Failed to load configuration", "error", err, "config_path", *configPath)
			os.Exit(1)
		}
	}

	continuousMode := cfg.Producer.MessageCount == 0
	slog.Info("Configuration loaded",
		"message_count", cfg.Producer.MessageCount,
		"workers", cfg.Producer.Workers,
		"output_format", cfg.Output.Format,
		"kafka_enabled", cfg.Kafka.Enabled,
		"continuous_mode", continuousMode,
	)

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("Shutdown signal received", "signal", sig.String())
		cancel()
	}()

	// Load reference data
	dataPath := cfg.Data.CurrencyRates[:len(cfg.Data.CurrencyRates)-len("/currency_rates.json")]
	slog.Info("Loading reference data", "data_path", dataPath)
	refData, err := generator.LoadReferenceData(dataPath)
	if err != nil {
		slog.Error("Failed to load reference data", "error", err)
		os.Exit(1)
	}
	slog.Info("Reference data loaded",
		"currencies", len(refData.Currencies),
		"currency_rates", len(refData.CurrencyRates),
		"agents", len(refData.Agents),
		"game_categories", len(refData.GameCategories),
	)

	// Initialize metrics monitor
	monitor := metrics.NewMonitor(cfg.Metrics.Interval, cfg.Metrics.Detailed, logger)
	doneCh := make(chan struct{})
	go monitor.StartReporting(doneCh)

	// Create transaction channel
	txnChan := make(chan *models.Transaction, cfg.Producer.BufferSize)

	// Initialize producer
	producer := generator.NewProducer(refData, logger)

	// Set up writers
	var wg sync.WaitGroup
	var writers []struct {
		name   string
		closer func() error
	}

	// Create output directory
	if err := os.MkdirAll(cfg.Output.Directory, 0755); err != nil {
		slog.Error("Failed to create output directory", "error", err, "directory", cfg.Output.Directory)
		os.Exit(1)
	}

	// CSV Writer
	if cfg.Output.CSV.Enabled && (cfg.Output.Format == "csv" || cfg.Output.Format == "both") {
		csvWriter, err := writer.NewCSVWriter(cfg.Output.Directory, cfg.Output.CSV.Filename, cfg.Output.CSV.BufferSize, logger)
		if err != nil {
			slog.Error("Failed to create CSV writer", "error", err)
			os.Exit(1)
		}
		writers = append(writers, struct {
			name   string
			closer func() error
		}{"CSV", csvWriter.Close})

		wg.Add(1)
		go func() {
			defer wg.Done()
			csvChan := make(chan *models.Transaction, cfg.Producer.BufferSize)
			go func() {
				for txn := range txnChan {
					csvChan <- txn
				}
				close(csvChan)
			}()
			
			if err := csvWriter.Write(ctx, csvChan); err != nil {
				slog.Error("CSV writer error", "error", err)
			}
			monitor.IncrementCSV(csvWriter.Count())
		}()
		
		slog.Info("CSV writer initialized",
			"directory", cfg.Output.Directory,
			"filename", cfg.Output.CSV.Filename,
		)
	}

	// Parquet Writer
	if cfg.Output.Parquet.Enabled && (cfg.Output.Format == "parquet" || cfg.Output.Format == "both") {
		parquetWriter, err := writer.NewParquetWriter(
			cfg.Output.Directory,
			cfg.Output.Parquet.Filename,
			cfg.Output.Parquet.RowGroupSize,
			cfg.Output.Parquet.Compression,
			logger,
		)
		if err != nil {
			slog.Error("Failed to create Parquet writer", "error", err)
			os.Exit(1)
		}
		writers = append(writers, struct {
			name   string
			closer func() error
		}{"Parquet", parquetWriter.Close})

		wg.Add(1)
		go func() {
			defer wg.Done()
			parquetChan := make(chan *models.Transaction, cfg.Producer.BufferSize)
			go func() {
				for txn := range txnChan {
					parquetChan <- txn
				}
				close(parquetChan)
			}()
			
			if err := parquetWriter.Write(ctx, parquetChan); err != nil {
				slog.Error("Parquet writer error", "error", err)
			}
			monitor.IncrementParquet(parquetWriter.Count())
		}()

		slog.Info("Parquet writer initialized",
			"directory", cfg.Output.Directory,
			"filename", cfg.Output.Parquet.Filename,
			"compression", cfg.Output.Parquet.Compression,
		)
	}

	// Kafka Writer
	if cfg.Kafka.Enabled {
		kafkaWriter, err := writer.NewKafkaWriter(
			cfg.Kafka.Brokers,
			cfg.Kafka.Topic,
			cfg.Kafka.Compression,
			cfg.Kafka.BatchSize,
			cfg.Kafka.FlushFrequency,
			cfg.Kafka.Async,
			logger,
		)
		if err != nil {
			slog.Error("Failed to create Kafka writer", "error", err)
			os.Exit(1)
		}
		writers = append(writers, struct {
			name   string
			closer func() error
		}{"Kafka", kafkaWriter.Close})

		wg.Add(1)
		go func() {
			defer wg.Done()
			kafkaChan := make(chan *models.Transaction, cfg.Producer.BufferSize)
			go func() {
				for txn := range txnChan {
					kafkaChan <- txn
				}
				close(kafkaChan)
			}()
			
			if err := kafkaWriter.Write(ctx, kafkaChan); err != nil {
				slog.Error("Kafka writer error", "error", err)
			}
			monitor.IncrementKafka(kafkaWriter.Count())
			monitor.IncrementKafkaErrors(kafkaWriter.Errors())
		}()
		
		slog.Info("Kafka writer initialized",
			"brokers", cfg.Kafka.Brokers,
			"topic", cfg.Kafka.Topic,
			"compression", cfg.Kafka.Compression,
		)
	}

	slog.Info("Starting message generation", "continuous_mode", continuousMode)

	// Start generation
	startTime := time.Now()
	
	if continuousMode {
		// Continuous mode - generate until stopped
		var totalGenerated atomic.Int64
		go func() {
			for {
				select {
				case <-ctx.Done():
					close(txnChan)
					return
				default:
					txn := producer.GenerateSingle()
					select {
					case txnChan <- txn:
						totalGenerated.Add(1)
					case <-ctx.Done():
						close(txnChan)
						return
					}
				}
			}
		}()

		// Update monitor periodically in continuous mode
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			var lastCount int64
			for {
				select {
				case <-ticker.C:
					current := totalGenerated.Load()
					monitor.IncrementTotal(current - lastCount)
					lastCount = current
				case <-ctx.Done():
					return
				}
			}
		}()

		// Wait for context cancellation
		<-ctx.Done()
	} else {
		// Fixed count mode
		go func() {
			if err := producer.Generate(ctx, cfg.Producer.MessageCount, cfg.Producer.Workers, txnChan); err != nil {
				slog.Error("Generation error", "error", err)
			}
			monitor.IncrementTotal(int64(cfg.Producer.MessageCount))
		}()
	}

	// Wait for writers to complete
	wg.Wait()
	
	// Stop metrics reporting
	close(doneCh)
	
	elapsed := time.Since(startTime)

	// Close all writers
	slog.Info("Closing writers", "count", len(writers))
	for _, w := range writers {
		if err := w.closer(); err != nil {
			slog.Error("Error closing writer", "writer", w.name, "error", err)
		} else {
			slog.Info("Writer closed", "writer", w.name)
		}
	}

	// Print final report
	monitor.FinalReport()
	
	slog.Info("Generation completed",
		"duration", elapsed.String(),
		"output_directory", cfg.Output.Directory,
	)
}
