package api

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	model2 "github.com/northstar-pay/nucleus/api/model"
	"github.com/northstar-pay/nucleus/internal/request"

	"github.com/northstar-pay/nucleus/model"

	"github.com/stretchr/testify/assert"
)

func TestCreateBalance(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	// Create a ledger for positive test case
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.CreateBalance
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Balance",
			payload: model2.CreateBalance{
				LedgerId: newLedger.LedgerID,
				Currency: gofakeit.Currency().Short,
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Missing Ledger ID",
			payload: model2.CreateBalance{
				Currency: gofakeit.Currency().Short,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Currency",
			payload: model2.CreateBalance{
				LedgerId: newLedger.LedgerID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Balance
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/balances",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if tt.expectedCode == http.StatusCreated {
				// Verify that the balance is actually created in the database
				balanceFromDB, err := b.GetBalanceByID(response.BalanceID, nil)
				if err != nil {
					t.Errorf("Failed to retrieve balance by ID: %v", err)
				} else {
					assert.Equal(t, response.BalanceID, balanceFromDB.BalanceID)
					assert.Equal(t, tt.payload.LedgerId, balanceFromDB.LedgerID)
					assert.Equal(t, tt.payload.Currency, balanceFromDB.Currency)
					assert.Equal(t, int64(0), balanceFromDB.Balance)
					assert.Equal(t, int64(0), balanceFromDB.DebitBalance)
					assert.Equal(t, int64(0), balanceFromDB.CreditBalance)
				}
			}
		})
	}
}

func TestGetBalance(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}
	newBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: gofakeit.CurrencyShort()})
	if err != nil {
		return
	}
	var response model.Balance
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/balances/%s", newBalance.BalanceID),
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, newBalance.Currency)
	assert.Equal(t, int64(0), newBalance.Balance)
	assert.Equal(t, int64(0), newBalance.DebitBalance)
	assert.Equal(t, int64(0), newBalance.CreditBalance)
	assert.Equal(t, int64(0), newBalance.InflightBalance)
	assert.Equal(t, int64(0), newBalance.InflightCreditBalance)
	assert.Equal(t, int64(0), newBalance.InflightDebitBalance)
	assert.Equal(t, int64(0), newBalance.Version)
}

func TestBalanceMonitor(t *testing.T) {
	//create balance
	//create monitor for when balance if greater than 1000
	//create monitor for when balance if less than 1000
	//create monitor for when balance

	//{
	//	"balance_id": "bln_0be360ca-86fe-457d-be43-daa3f966d8f0",
	//	"condition": {
	//	"field": "debit_balance",
	//		"operator": ">",
	//		"value": 100000
	//},
	//	"description": "Tier 1 Account"
	//}

}
