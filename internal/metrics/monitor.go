package metrics

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Monitor tracks and reports performance metrics
type Monitor struct {
	startTime      time.Time
	totalMessages  atomic.Int64
	lastMessages   atomic.Int64
	lastReportTime atomic.Value // stores time.Time
	interval       time.Duration
	detailed       bool
	mu             sync.Mutex
	logger         *slog.Logger
	
	// Writer-specific counters
	csvCount     atomic.Int64
	parquetCount atomic.Int64
	kafkaCount   atomic.Int64
	kafkaErrors  atomic.Int64
}

// NewMonitor creates a new performance monitor
func NewMonitor(interval int, detailed bool, logger *slog.Logger) *Monitor {
	m := &Monitor{
		startTime: time.Now(),
		interval:  time.Duration(interval) * time.Second,
		detailed:  detailed,
		logger:    logger,
	}
	m.lastReportTime.Store(time.Now())
	return m
}

// IncrementTotal increments the total message counter
func (m *Monitor) IncrementTotal(count int64) {
	m.totalMessages.Add(count)
}

// IncrementCSV increments the CSV writer counter
func (m *Monitor) IncrementCSV(count int64) {
	m.csvCount.Add(count)
}

// IncrementParquet increments the Parquet writer counter
func (m *Monitor) IncrementParquet(count int64) {
	m.parquetCount.Add(count)
}

// IncrementKafka increments the Kafka writer counter
func (m *Monitor) IncrementKafka(count int64) {
	m.kafkaCount.Add(count)
}

// IncrementKafkaErrors increments the Kafka error counter
func (m *Monitor) IncrementKafkaErrors(count int64) {
	m.kafkaErrors.Add(count)
}

// Report generates and prints a performance report
func (m *Monitor) Report() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	now := time.Now()
	lastReport := m.lastReportTime.Load().(time.Time)
	
	elapsed := now.Sub(m.startTime).Seconds()
	intervalElapsed := now.Sub(lastReport).Seconds()
	
	total := m.totalMessages.Load()
	current := total - m.lastMessages.Load()
	
	// Calculate rates
	overallRate := float64(total) / elapsed
	intervalRate := float64(current) / intervalElapsed
	
	// Log metrics
	m.logger.Info("Performance metrics",
		"total_messages", total,
		"elapsed", formatDuration(time.Since(m.startTime)),
		"overall_rate", formatRate(overallRate),
		"current_rate", formatRate(intervalRate),
	)
	
	if m.detailed {
		m.logger.Info("Writer metrics",
			"csv", m.csvCount.Load(),
			"parquet", m.parquetCount.Load(),
			"kafka", m.kafkaCount.Load(),
			"kafka_errors", m.kafkaErrors.Load(),
		)
	}
	
	// Update for next report
	m.lastMessages.Store(total)
	m.lastReportTime.Store(now)
}

// FinalReport prints the final performance summary
func (m *Monitor) FinalReport() {
	elapsed := time.Since(m.startTime)
	total := m.totalMessages.Load()
	rate := float64(total) / elapsed.Seconds()
	
	// Log final summary
	m.logger.Info("Final summary",
		"total_messages", total,
		"total_time", formatDuration(elapsed),
		"average_throughput", formatRate(rate),
	)
	
	if m.detailed {
		m.logger.Info("Output breakdown",
			"csv", m.csvCount.Load(),
			"parquet", m.parquetCount.Load(),
			"kafka", m.kafkaCount.Load(),
			"kafka_errors", m.kafkaErrors.Load(),
		)
	}
	
	// Performance assessment
	var assessment string
	if rate >= 30000 {
		assessment = "EXCELLENT: Exceeded 30K messages/sec target"
	} else if rate >= 20000 {
		assessment = "GOOD: Met 20K messages/sec target"
	} else if rate >= 10000 {
		assessment = "MODERATE: Performance below target (10K-20K/sec)"
	} else {
		assessment = "LOW: Performance significantly below target (<10K/sec)"
	}
	
	m.logger.Info("Performance assessment", "result", assessment, "rate_msg_per_sec", int64(rate))
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

func formatRate(rate float64) string {
	if rate >= 1000000 {
		return fmt.Sprintf("%.2fM msg/sec", rate/1000000)
	} else if rate >= 1000 {
		return fmt.Sprintf("%.2fK msg/sec", rate/1000)
	}
	return fmt.Sprintf("%.0f msg/sec", rate)
}

// StartReporting starts periodic metric reporting
func (m *Monitor) StartReporting(done <-chan struct{}) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			m.Report()
		case <-done:
			return
		}
	}
}
