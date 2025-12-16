# High-Performance Message Producer

A production-grade Go-based message producer capable of generating 300K+ messages per second with support for multiple output formats (CSV, Parquet) and optional Kafka streaming.

## Features

- High Performance: Generates 300K+ messages/sec using concurrent goroutines and worker pools
- Multiple Output Formats: CSV, Parquet, or both simultaneously with toggle support
- Kafka Integration: Optional Kafka streaming with configurable compression
- Structured Logging: JSON-formatted logs using Go's standard log/slog package
- Real-time Metrics: Live throughput monitoring and performance reporting
- Realistic Data: Uses actual currency rates, agents, and game categories
- Clean Architecture: Industry-standard project structure and design patterns
- Highly Configurable: YAML-based configuration for all aspects
- Continuous Mode: Support for infinite message generation until stopped
- Docker Support: Multi-stage builds with Docker Compose orchestration

## Project Structure

```
mesage_producer/
├── cmd/
│   └── producer/
│       └── main.go              # Application entry point
├── internal/
│   ├── config/
│   │   └── loader.go            # Configuration management
│   ├── models/
│   │   └── models.go            # Data models
│   ├── generator/
│   │   └── producer.go          # Message generation logic
│   ├── writer/
│   │   ├── csv.go               # CSV output writer
│   │   ├── parquet.go           # Parquet output writer
│   │   └── kafka.go             # Kafka streaming writer
│   └── metrics/
│       └── monitor.go           # Performance monitoring
├── data/
│   ├── currency_rates.json      # Currency conversion rates
│   ├── agents.json              # Agent configuration
│   ├── game_categories.json     # Game categories
│   └── currencies.json          # Currency definitions
├── config.yaml                  # Default configuration
├── config.continuous.yaml       # Continuous mode config
├── config.kafka.yaml            # Kafka streaming config
├── Dockerfile                   # Docker multi-stage build
├── docker-compose.yaml          # Full stack with Kafka
├── Makefile                     # Build automation
└── README.md                    # This file
```

## Prerequisites

- Go 1.23 or higher
- (Optional) Docker and Docker Compose
- (Optional) Kafka cluster for streaming output

## Quick Start

### Using Make

```bash
# Build the application
make build

# Run with default config
make run

# Clean output files
make clean

# Run tests
make test
```

### Using Docker

```bash
# Build Docker image
docker build -t message-producer:latest .

# Run with default config
docker run --rm -v $(pwd)/output:/app/output message-producer:latest

# Run with custom config
docker run --rm \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/config.continuous.yaml:/app/config.yaml \
  message-producer:latest
```

### Using Docker Compose (with Kafka)

```bash
# Start full stack (Zookeeper + Kafka + Producer)
docker-compose up

# Run in detached mode
docker-compose up -d

# View logs
docker-compose logs -f producer

# Stop and remove containers
docker-compose down
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/SupratickDey/mesage_producer.git
cd mesage_producer
```

2. Download dependencies:
```bash
go mod download
```

3. Build the application:
```bash
make build
# or
go build -o producer ./cmd/producer
```

## Configuration

The application supports multiple configuration files for different scenarios:

- `config.yaml`: Default configuration (100K messages, both CSV and Parquet)
- `config.continuous.yaml`: Continuous mode (infinite generation until stopped)
- `config.kafka.yaml`: Kafka streaming with file output disabled

### Configuration Options

Edit configuration files to customize:

- **Message count**: Number of messages to generate (0 = continuous mode)
- **Workers**: Number of concurrent goroutines
- **Buffer size**: Channel buffer size for throughput optimization
- **Output format**: `csv`, `parquet`, or `both`
- **CSV/Parquet enabled**: Toggle individual output formats on/off
- **Kafka**: Enable/disable and configure Kafka settings
- **Compression**: Choose compression algorithm (snappy, gzip, lz4, zstd)

### Example Configuration

```yaml
producer:
  message_count: 100000    # 0 for continuous mode
  workers: 10
  buffer_size: 10000

output:
  format: "both"
  directory: "./output"
  
  csv:
    enabled: true          # Toggle CSV output
    filename: "transactions.csv"
    buffer_size: 100
  
  parquet:
    enabled: true          # Toggle Parquet output
    filename: "transactions.parquet"
    row_group_size: 10000
    compression: "snappy"

kafka:
  enabled: false           # Toggle Kafka streaming
  brokers: ["localhost:9092"]
  topic: "transactions"
  compression: "snappy"
```

### Output Format Control

You can now enable/disable CSV and Parquet output independently:

```yaml
# CSV only
csv:
  enabled: true
parquet:
  enabled: false

# Parquet only
csv:
  enabled: false
parquet:
  enabled: true

# Kafka only (no file output)
csv:
  enabled: false
parquet:
  enabled: false
kafka:
  enabled: true
```

## Usage

### Standard Mode

```bash
# Run with default config (100K messages)
./producer

# Run with custom config
./producer -config config.yaml

# Run in continuous mode (Ctrl+C to stop)
./producer -config config.continuous.yaml

# Run with Kafka enabled
./producer -config config.kafka.yaml
```

### Direct Execution

```bash
# Run without building
go run ./cmd/producer/main.go

# With custom config
go run ./cmd/producer/main.go -config config.continuous.yaml
```

### Logging

The application uses structured JSON logging:

```json
{"time":"2025-12-16T12:00:00Z","level":"INFO","msg":"Starting message producer","version":"1.0.0"}
{"time":"2025-12-16T12:00:05Z","level":"INFO","msg":"Performance metrics","total_messages":1000000,"elapsed":"5.00s","overall_rate":"200.00K msg/sec"}
{"time":"2025-12-16T12:00:10Z","level":"INFO","msg":"Final summary","total_messages":5000000,"total_time":"25.00s","average_throughput":"200.00K msg/sec"}
```

## Performance

Tested performance on modern hardware (Apple Silicon / Multi-core x86):

- **Parquet (Snappy)**: 300K-400K msg/sec
- **CSV**: 100K-200K msg/sec
- **Both formats**: 150K-250K msg/sec
- **Kafka streaming**: 200K-300K msg/sec

Performance factors:
- CPU cores and clock speed
- Disk I/O performance (SSD vs HDD)
- Network latency (for Kafka)
- Compression settings
- Worker count and buffer sizes

### Continuous Mode Performance

When running in continuous mode (`message_count: 0`), the producer generates messages indefinitely until stopped with Ctrl+C or SIGTERM. Throughput remains consistent over extended periods with proper resource allocation.

## Output

### CSV Format
Human-readable format with headers, suitable for analysis in Excel or pandas.

### Parquet Format
Columnar storage format with compression, optimized for big data analytics. Ideal for:
- Data lakes (S3, HDFS)
- Analytics platforms (Spark, Presto)
- Data warehouses (Snowflake, BigQuery)

### Kafka Streaming
Real-time message streaming for:
- Event-driven architectures
- Stream processing (Kafka Streams, Flink)
- Real-time analytics

## Data Model

Transactions include:
- Transaction IDs (internal and external)
- Vendor information
- Game category
- Agent hierarchy (master agent → agent)
- Currency and amounts (bet, win, win/loss)
- Timestamps

All data relationships are maintained based on actual reference data from `data/` directory.

## Monitoring

Real-time metrics are logged every 5 seconds in JSON format:

```json
{
  "time": "2025-12-16T12:00:05Z",
  "level": "INFO",
  "msg": "Performance metrics",
  "total_messages": 1500000,
  "elapsed": "5.00s",
  "overall_rate": "300.00K msg/sec",
  "current_rate": "320.00K msg/sec"
}
```

Final report includes:
- Total messages generated
- Total execution time
- Average throughput
- Performance assessment (EXCELLENT/GOOD/MODERATE/LOW)
- Output breakdown by writer
- Error counts (if any)

### Detailed Metrics

When detailed logging is enabled, writer-specific metrics are included:

```json
{
  "time": "2025-12-16T12:00:05Z",
  "level": "INFO",
  "msg": "Writer metrics",
  "csv": 500000,
  "parquet": 1000000,
  "kafka": 1500000,
  "kafka_errors": 0
}
```

## Architecture Highlights

### Concurrency Pattern
- **Producer**: Multiple workers generate messages concurrently
- **Fan-out**: Single channel distributes to multiple writers
- **Buffering**: Configurable channel buffers prevent blocking

### Performance Optimizations
- **Zero-copy**: Direct struct mapping to Parquet
- **Batch writes**: Configurable buffer sizes
- **Async Kafka**: Non-blocking message production
- **Memory pooling**: Efficient buffer reuse

### Error Handling
- **Graceful shutdown**: SIGINT/SIGTERM handling
- **Context cancellation**: Proper cleanup on errors
- **Writer isolation**: Individual writer failures don't affect others

## Troubleshooting

### Low Performance

- Increase `workers` count (typically 2x CPU cores)
- Increase `buffer_size` for better throughput
- Use `parquet` format instead of `csv` for faster writes
- Reduce compression level or use `snappy` (fastest)
- Check disk I/O with `iostat` or Activity Monitor
- Ensure adequate RAM for buffering

### Kafka Connection Errors

- Verify broker connectivity: `telnet localhost 9092`
- Check topic exists: `kafka-topics --list --bootstrap-server localhost:9092`
- Review broker logs for authentication or permission issues
- Increase `batch_size` and `flush_frequency` for better throughput
- Try different compression settings

### Memory Issues

- Reduce `buffer_size` to lower memory consumption
- Decrease `row_group_size` for Parquet (trades compression for memory)
- Process in smaller batches instead of continuous mode
- Monitor with `go tool pprof` for memory profiling

### Docker Issues

- Ensure adequate resources allocated to Docker (CPU, Memory)
- Check volume mounts have write permissions
- Review container logs: `docker logs <container-id>`
- For Compose: verify Kafka health with `docker-compose ps`

## Development

### Building

```bash
# Build for current platform
make build

# Build with specific flags
go build -ldflags="-s -w" -o producer ./cmd/producer

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o producer-linux ./cmd/producer
```

### Testing

```bash
# Run all tests
make test

# Run with coverage
go test -cover ./...

# Run specific package tests
go test ./internal/generator/...
```

### Code Structure

The project follows clean architecture principles:

- `cmd/producer`: Application entry point, CLI handling
- `internal/config`: Configuration loading and validation
- `internal/models`: Data structures and types
- `internal/generator`: Core message generation logic
- `internal/writer`: Output writers (CSV, Parquet, Kafka)
- `internal/metrics`: Performance monitoring and reporting
- `data/`: Reference data in JSON format

## License

MIT License

## Contributing

Contributions are welcome. Please submit pull requests or open issues for bugs and feature requests.
