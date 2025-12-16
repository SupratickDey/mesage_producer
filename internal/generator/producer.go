package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
	"github.com/supratick/message_producer/internal/models"
)

// Producer generates transaction messages
type Producer struct {
	refData        *models.ReferenceData
	sequence       atomic.Int64
	rng            *rand.Rand
	mu             sync.Mutex
	vendorCodes    []string
	betAmounts     []decimal.Decimal
	winMultipliers []float64
	logger         *slog.Logger
}

// NewProducer creates a new message producer
func NewProducer(refData *models.ReferenceData, logger *slog.Logger) *Producer {
	return &Producer{
		refData:     refData,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		vendorCodes: []string{"PRAGMATIC", "EVOLUTION", "NETENT", "MICROGAMING", "PLAYTECH", "EGT", "PLAYSON"},
		betAmounts: []decimal.Decimal{
			decimal.NewFromFloat(10.0),
			decimal.NewFromFloat(50.0),
			decimal.NewFromFloat(100.0),
			decimal.NewFromFloat(200.0),
			decimal.NewFromFloat(500.0),
			decimal.NewFromFloat(1000.0),
		},
		winMultipliers: []float64{0, 0, 0.5, 0.8, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0}, // More losses than wins
		logger:         logger,
	}
}

// LoadReferenceData loads all reference data from files
func LoadReferenceData(dataPath string) (*models.ReferenceData, error) {
	rd := &models.ReferenceData{
		CurrencyByID:      make(map[int]*models.Currency),
		CurrencyRatesByID: make(map[int][]models.CurrencyRate),
		AgentsByMasterID:  make(map[int][]models.Agent),
	}

	// Load currencies
	currencies, err := loadCurrencies(dataPath + "/currencies.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load currencies: %w", err)
	}
	rd.Currencies = currencies
	for i := range currencies {
		rd.CurrencyByID[currencies[i].ID] = &currencies[i]
	}

	// Load currency rates
	currencyRates, err := loadCurrencyRates(dataPath + "/currency_rates.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load currency rates: %w", err)
	}
	rd.CurrencyRates = currencyRates
	for _, rate := range currencyRates {
		rd.CurrencyRatesByID[rate.CurrencyFromID] = append(rd.CurrencyRatesByID[rate.CurrencyFromID], rate)
	}

	// Load agents
	agents, err := loadAgents(dataPath + "/agents.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load agents: %w", err)
	}
	rd.Agents = agents
	for _, agent := range agents {
		rd.AgentsByMasterID[agent.MasterAgentID] = append(rd.AgentsByMasterID[agent.MasterAgentID], agent)
	}

	// Load game categories
	gameCategories, err := loadGameCategories(dataPath + "/game_categories.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load game categories: %w", err)
	}
	rd.GameCategories = gameCategories

	return rd, nil
}

func loadCurrencies(path string) ([]models.Currency, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var currencies []models.Currency
	if err := json.Unmarshal(data, &currencies); err != nil {
		return nil, err
	}
	return currencies, nil
}

func loadCurrencyRates(path string) ([]models.CurrencyRate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	
	var rawRates []map[string]interface{}
	if err := json.Unmarshal(data, &rawRates); err != nil {
		return nil, err
	}
	
	rates := make([]models.CurrencyRate, len(rawRates))
	for i, raw := range rawRates {
		rates[i] = models.CurrencyRate{
			ID:             int(raw["id"].(float64)),
			CurrencyFrom:   raw["currency_from"].(string),
			CurrencyFromID: int(raw["currency_from_id"].(float64)),
			CurrencyTo:     raw["currency_to"].(string),
			CurrencyToID:   int(raw["currency_to_id"].(float64)),
			Rate:           decimal.NewFromFloat(raw["rate"].(float64)),
			EffectiveFrom:  int64(raw["effective_from"].(float64)),
			Status:         int(raw["status"].(float64)),
		}
	}
	return rates, nil
}

func loadAgents(path string) ([]models.Agent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var agents []models.Agent
	if err := json.Unmarshal(data, &agents); err != nil {
		return nil, err
	}
	return agents, nil
}

func loadGameCategories(path string) ([]models.GameCategory, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var categories []models.GameCategory
	if err := json.Unmarshal(data, &categories); err != nil {
		return nil, err
	}
	return categories, nil
}

// GenerateSingle generates a single transaction
func (p *Producer) GenerateSingle() *models.Transaction {
	p.mu.Lock()
	txn := p.generateTransaction(p.rng)
	p.mu.Unlock()
	return txn
}

// Generate produces transactions and sends them to the output channel
func (p *Producer) Generate(ctx context.Context, count int, workers int, output chan<- *models.Transaction) error {
	var wg sync.WaitGroup
	messagesPerWorker := count / workers

	for i := 0; i < workers; i++ {
		wg.Add(1)
		start := i * messagesPerWorker
		end := start + messagesPerWorker
		if i == workers-1 {
			end = count // Last worker handles remainder
		}

		go func(start, end int) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(start)))
			
			for j := start; j < end; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					txn := p.generateTransaction(localRng)
					output <- txn
				}
			}
		}(start, end)
	}

	wg.Wait()
	close(output)
	return nil
}

func (p *Producer) generateTransaction(rng *rand.Rand) *models.Transaction {
	seq := p.sequence.Add(1)
	now := time.Now()
	
	// Select random data
	currency := p.refData.Currencies[rng.Intn(len(p.refData.Currencies))]
	gameCategory := p.refData.GameCategories[rng.Intn(len(p.refData.GameCategories))]
	
	// Select master agent and then one of its agents
	var agent models.Agent
	masterAgentIDs := make([]int, 0, len(p.refData.AgentsByMasterID))
	for k := range p.refData.AgentsByMasterID {
		masterAgentIDs = append(masterAgentIDs, k)
	}
	masterAgentID := masterAgentIDs[rng.Intn(len(masterAgentIDs))]
	agents := p.refData.AgentsByMasterID[masterAgentID]
	agent = agents[rng.Intn(len(agents))]
	
	vendorCode := p.vendorCodes[rng.Intn(len(p.vendorCodes))]
	vendorID := rng.Intn(10) + 1
	
	// Generate bet amount based on currency
	betAmount := p.betAmounts[rng.Intn(len(p.betAmounts))]
	
	// Adjust for currency (crypto gets smaller amounts, fiat gets larger)
	if currency.Code == "BTC" {
		betAmount = betAmount.Div(decimal.NewFromFloat(10000))
	} else if currency.Code == "ETH" {
		betAmount = betAmount.Div(decimal.NewFromFloat(1000))
	} else if currency.Code == "JPY" {
		betAmount = betAmount.Mul(decimal.NewFromFloat(100))
	} else if currency.Code == "CNY" {
		betAmount = betAmount.Mul(decimal.NewFromFloat(7))
	}
	
	// Generate win amount (weighted towards losses)
	winMultiplier := p.winMultipliers[rng.Intn(len(p.winMultipliers))]
	winAmount := betAmount.Mul(decimal.NewFromFloat(winMultiplier))
	winLoss := winAmount.Sub(betAmount)
	
	return &models.Transaction{
		ID:                    fmt.Sprintf("TXN-%s-%08d", now.Format("20060102"), seq),
		ExternalTransactionID: fmt.Sprintf("EXT-%s-%08d", vendorCode, seq),
		VendorBetID:           fmt.Sprintf("BET-%08d", seq),
		RoundID:               fmt.Sprintf("ROUND-%08d", seq/10), // Multiple bets per round
		VendorID:              vendorID,
		VendorCode:            vendorCode,
		VendorLineID:          1,
		GameCategoryID:        gameCategory.ID,
		HouseID:               1,
		MasterAgentID:         agent.MasterAgentID,
		AgentID:               agent.ID,
		CurrencyID:            currency.ID,
		CurrencyCode:          currency.Code,
		BetAmount:             betAmount.StringFixed(6),
		WinAmount:             winAmount.StringFixed(6),
		WinLoss:               winLoss.StringFixed(6),
		SettledAt:             now.Format(time.RFC3339),
	}
}
