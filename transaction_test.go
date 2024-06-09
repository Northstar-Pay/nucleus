package blnk

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/northstar-pay/nucleus/model"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/stretchr/testify/assert"
)

func TestRecordTransaction(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	source := gofakeit.UUID()
	destination := gofakeit.UUID()

	txn := &model.Transaction{
		Reference:      gofakeit.UUID(),
		Source:         source,
		Destination:    destination,
		Amount:         10,
		AllowOverdraft: false,
		Precision:      100,
		Currency:       "NGN",
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(source, "NGN", 1, "ledger-id-source", 10000, 10000, 0, 0, 0, 0, time.Now(), 0)

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(destination, "NGN", 1, "ledger-id-destination", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	balanceQuery := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = $1`)
	balanceQuery2 := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = $1`)

	mock.ExpectQuery(balanceQuery).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQuery2).WithArgs(destination).WillReturnRows(destinationBalanceRows)
	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
	`)).WithArgs(
		source,
		9000,
		10000,
		1000,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
	`)).WithArgs(
		destination,
		1000,
		1000,
		0,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()
	expectedSQL := `INSERT INTO blnk.transactions(transaction_id,parent_transaction,source,reference,amount,precise_amount,precision,rate,currency,destination,description,status,created_at,meta_data,scheduled_for,hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`
	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		source,
		txn.Reference,
		txn.Amount,
		1000,
		txn.Precision,
		float64(1),
		txn.Currency,
		txn.Destination,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = d.RecordTransaction(context.Background(), txn)
	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestRecordTransactionWithRate(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	source := gofakeit.UUID()
	destination := gofakeit.UUID()

	txn := &model.Transaction{
		Reference:      gofakeit.UUID(),
		Source:         source,
		Destination:    destination,
		Amount:         1000000,
		Rate:           1300,
		AllowOverdraft: true,
		Precision:      100,
		Currency:       "NGN",
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(source, "USD", 1, "ledger-id-source", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(destination, "NGN", 1, "ledger-id-destination", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	balanceQuery := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = $1`)
	balanceQuery2 := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = $1`)

	mock.ExpectQuery(balanceQuery).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQuery2).WithArgs(destination).WillReturnRows(destinationBalanceRows)
	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
	`)).WithArgs(
		source,
		-100000000,
		0,
		100000000,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		"USD",
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
	`)).WithArgs(
		destination,
		130000000000,
		130000000000,
		0,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		"NGN",
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()
	expectedSQL := `INSERT INTO blnk.transactions(transaction_id,parent_transaction,source,reference,amount,precise_amount,precision,rate,currency,destination,description,status,created_at,meta_data,scheduled_for,hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`
	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		source,
		txn.Reference,
		txn.Amount,
		100000000,
		txn.Precision,
		float64(1300),
		txn.Currency,
		txn.Destination,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = d.RecordTransaction(context.Background(), txn)
	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
