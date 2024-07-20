package api

import (
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	model2 "github.com/northstar-pay/nucleus/api/model"
	"github.com/northstar-pay/nucleus/internal/request"

	"github.com/northstar-pay/nucleus/model"

	"github.com/stretchr/testify/assert"
)

func TestRecordTransaction(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	// Create ledger and balances for testing
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newSourceBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Fatalf("Failed to create source balance: %v", err)
	}

	newDestinationBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Fatalf("Failed to create destination balance: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.RecordTransaction
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Transaction",
			payload: model2.RecordTransaction{
				Amount:      750,
				Precision:   100,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Missing Amount",
			payload: model2.RecordTransaction{
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Reference",
			payload: model2.RecordTransaction{
				Amount:      750,
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Currency",
			payload: model2.RecordTransaction{
				Amount:      750,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Source",
			payload: model2.RecordTransaction{
				Amount:      750,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Destination",
			payload: model2.RecordTransaction{
				Amount:      750,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Transaction
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/transactions",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if !tt.wantErr && tt.expectedCode == http.StatusCreated {
				assert.Equal(t, tt.payload.Amount, response.Amount)
				assert.Equal(t, int64(tt.payload.Precision*tt.payload.Amount), response.PreciseAmount)
				assert.Equal(t, tt.payload.Reference, response.Reference)
				assert.Equal(t, tt.payload.Description, response.Description)
				assert.Equal(t, tt.payload.Currency, response.Currency)
				assert.Equal(t, tt.payload.Source, response.Source)
				assert.Equal(t, tt.payload.Destination, response.Destination)
				assert.Equal(t, "QUEUED", response.Status)
			}
		})
	}
}

func TestRecordTransactionWithExitingRef(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newSourceBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}

	newDestinationBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}
	validPayload := model2.RecordTransaction{
		Amount:      10000,
		Reference:   gofakeit.UUID(),
		Description: "test",
		Currency:    "NGN",
		Destination: newDestinationBalance.BalanceID,
		Source:      newSourceBalance.BalanceID,
	}
	payloadBytes, _ := request.ToJsonReq(&validPayload)
	var response model.Transaction
	testRequest := TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, http.StatusCreated, resp.Code)
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.Equal(t, response.Status, "QUEUED")
}
