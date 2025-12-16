package writer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/supratick/message_producer/internal/models"
)

// ParquetWriter writes transactions to Parquet file
type ParquetWriter struct {
	file         *os.File
	writer       *parquet.GenericWriter[*models.Transaction]
	rowGroupSize int
	buffer       []*models.Transaction
	count        atomic.Int64
	logger       *slog.Logger
}

// NewParquetWriter creates a new Parquet writer
func NewParquetWriter(outputDir, filename string, rowGroupSize int, compression string, logger *slog.Logger) (*ParquetWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	path := filepath.Join(outputDir, filename)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet file: %w", err)
	}

	// Configure compression
	var compressionCodec compress.Codec
	switch compression {
	case "snappy":
		compressionCodec = &parquet.Snappy
	case "gzip":
		compressionCodec = &parquet.Gzip
	case "lz4":
		compressionCodec = &parquet.Lz4Raw
	case "zstd":
		compressionCodec = &parquet.Zstd
	default:
		compressionCodec = &parquet.Uncompressed
	}

	// Create writer with schema
	writer := parquet.NewGenericWriter[*models.Transaction](
		file,
		parquet.Compression(compressionCodec),
		parquet.PageBufferSize(1024*1024), // 1MB page buffer
	)

	return &ParquetWriter{
		file:         file,
		writer:       writer,
		rowGroupSize: rowGroupSize,
		buffer:       make([]*models.Transaction, 0, rowGroupSize),
		logger:       logger,
	}, nil
}

// Write writes transactions from the channel to Parquet
func (w *ParquetWriter) Write(ctx context.Context, input <-chan *models.Transaction) error {
	for {
		select {
		case <-ctx.Done():
			return w.flush()
		case txn, ok := <-input:
			if !ok {
				// Channel closed, flush remaining buffer
				return w.flush()
			}
			
			w.buffer = append(w.buffer, txn)
			if len(w.buffer) >= w.rowGroupSize {
				if err := w.flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (w *ParquetWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	n, err := w.writer.Write(w.buffer)
	if err != nil {
		return fmt.Errorf("failed to write to Parquet: %w", err)
	}

	w.count.Add(int64(n))
	w.buffer = w.buffer[:0]
	return nil
}

// Close closes the Parquet writer
func (w *ParquetWriter) Close() error {
	if err := w.flush(); err != nil {
		w.writer.Close()
		w.file.Close()
		return err
	}
	
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}
	
	return w.file.Close()
}

// Count returns the number of transactions written
func (w *ParquetWriter) Count() int64 {
	return w.count.Load()
}
