package writer

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/supratick/message_producer/internal/models"
)

// CSVWriter writes transactions to CSV file
type CSVWriter struct {
	file       *os.File
	writer     *csv.Writer
	bufferSize int
	buffer     []*models.Transaction
	count      atomic.Int64
	logger     *slog.Logger
}

// NewCSVWriter creates a new CSV writer
func NewCSVWriter(outputDir, filename string, bufferSize int, logger *slog.Logger) (*CSVWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	path := filepath.Join(outputDir, filename)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	writer := csv.NewWriter(file)
	
	// Write header
	header := []string{
		"id", "external_transaction_id", "vendor_bet_id", "round_id",
		"vendor_id", "vendor_code", "vendor_line_id", "game_category_id",
		"house_id", "master_agent_id", "agent_id", "currency_id",
		"currency_code", "bet_amount", "win_amount", "win_loss", "settled_at",
	}
	if err := writer.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	return &CSVWriter{
		file:       file,
		writer:     writer,
		bufferSize: bufferSize,
		buffer:     make([]*models.Transaction, 0, bufferSize),
		logger:     logger,
	}, nil
}

// Write writes transactions from the channel to CSV
func (w *CSVWriter) Write(ctx context.Context, input <-chan *models.Transaction) error {
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
			if len(w.buffer) >= w.bufferSize {
				if err := w.flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (w *CSVWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	for _, txn := range w.buffer {
		record := []string{
			txn.ID,
			txn.ExternalTransactionID,
			txn.VendorBetID,
			txn.RoundID,
			fmt.Sprintf("%d", txn.VendorID),
			txn.VendorCode,
			fmt.Sprintf("%d", txn.VendorLineID),
			fmt.Sprintf("%d", txn.GameCategoryID),
			fmt.Sprintf("%d", txn.HouseID),
			fmt.Sprintf("%d", txn.MasterAgentID),
			fmt.Sprintf("%d", txn.AgentID),
			fmt.Sprintf("%d", txn.CurrencyID),
			txn.CurrencyCode,
			txn.BetAmount,
			txn.WinAmount,
			txn.WinLoss,
			txn.SettledAt,
		}
		
		if err := w.writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}
	
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}
	
	w.count.Add(int64(len(w.buffer)))
	w.buffer = w.buffer[:0]
	return nil
}

// Close closes the CSV writer
func (w *CSVWriter) Close() error {
	if err := w.flush(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

// Count returns the number of transactions written
func (w *CSVWriter) Count() int64 {
	return w.count.Load()
}
