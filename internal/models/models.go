package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Transaction represents a betting transaction
type Transaction struct {
	ID                    string          `json:"id" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	ExternalTransactionID string          `json:"external_transaction_id" parquet:"name=external_transaction_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	VendorBetID           string          `json:"vendor_bet_id" parquet:"name=vendor_bet_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	RoundID               string          `json:"round_id" parquet:"name=round_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	VendorID              int             `json:"vendor_id" parquet:"name=vendor_id, type=INT32"`
	VendorCode            string          `json:"vendor_code" parquet:"name=vendor_code, type=BYTE_ARRAY, convertedtype=UTF8"`
	VendorLineID          int             `json:"vendor_line_id" parquet:"name=vendor_line_id, type=INT32"`
	GameCategoryID        int             `json:"game_category_id" parquet:"name=game_category_id, type=INT32"`
	HouseID               int             `json:"house_id" parquet:"name=house_id, type=INT32"`
	MasterAgentID         int             `json:"master_agent_id" parquet:"name=master_agent_id, type=INT32"`
	AgentID               int             `json:"agent_id" parquet:"name=agent_id, type=INT32"`
	CurrencyID            int             `json:"currency_id" parquet:"name=currency_id, type=INT32"`
	CurrencyCode          string          `json:"currency_code" parquet:"name=currency_code, type=BYTE_ARRAY, convertedtype=UTF8"`
	BetAmount             string          `json:"bet_amount" parquet:"name=bet_amount, type=BYTE_ARRAY, convertedtype=UTF8"`
	WinAmount             string          `json:"win_amount" parquet:"name=win_amount, type=BYTE_ARRAY, convertedtype=UTF8"`
	WinLoss               string          `json:"win_loss" parquet:"name=win_loss, type=BYTE_ARRAY, convertedtype=UTF8"`
	SettledAt             string          `json:"settled_at" parquet:"name=settled_at, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// CurrencyRate represents a currency conversion rate
type CurrencyRate struct {
	ID             int             `json:"id"`
	CurrencyFrom   string          `json:"currency_from"`
	CurrencyFromID int             `json:"currency_from_id"`
	CurrencyTo     string          `json:"currency_to"`
	CurrencyToID   int             `json:"currency_to_id"`
	Rate           decimal.Decimal `json:"rate"`
	EffectiveFrom  int64           `json:"effective_from"`
	Status         int             `json:"status"`
}

// Agent represents an agent entity
type Agent struct {
	ID                  int `json:"id"`
	SASEntityID         int `json:"sas_entity_id"`
	MasterAgentID       int `json:"master_agent_id"`
	Status              int `json:"status"`
	NotificationEnabled int `json:"notification_enabled"`
}

// GameCategory represents a game category
type GameCategory struct {
	ID     int    `json:"id"`
	Code   string `json:"code"`
	Name   string `json:"name"`
	Status int    `json:"status"`
}

// Currency represents a currency
type Currency struct {
	ID   int    `json:"id"`
	Code string `json:"code"`
	Name string `json:"name"`
}

// ReferenceData holds all reference data needed for message generation
type ReferenceData struct {
	CurrencyRates  []CurrencyRate
	Agents         []Agent
	GameCategories []GameCategory
	Currencies     []Currency
	
	// Index maps for fast lookups
	CurrencyByID       map[int]*Currency
	CurrencyRatesByID  map[int][]CurrencyRate
	AgentsByMasterID   map[int][]Agent
}

// TransactionMetadata holds metadata for generating transactions
type TransactionMetadata struct {
	SequenceNumber int64
	Timestamp      time.Time
}
