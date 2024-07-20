package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/northstar-pay/nucleus/model"
)

func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

func prepareQueries(queryBuilder strings.Builder, include []string) string {
	var selectFields []string
	// Default fields for balances
	selectFields = append(selectFields,
		"b.balance_id", "b.balance", "b.credit_balance", "b.debit_balance",
		"b.currency", "b.currency_multiplier", "b.ledger_id",
		"COALESCE(b.identity_id, '') as identity_id", "b.created_at", "b.meta_data", "b.inflight_balance", "b.inflight_credit_balance", "b.inflight_debit_balance", "b.version")

	// Append fields and joins based on 'include'
	if contains(include, "identity") {
		selectFields = append(selectFields,
			"i.identity_id", "i.first_name", "i_name", "i.category", "i.last_name", "i.other_names",
			"i.gender", "i.dob", "i.email_address", "i.phone_number",
			"i.nationality", "i.street", "i.country", "i.state",
			"i.post_code", "i.city", "i.created_at")
	}
	if contains(include, "ledger") {
		selectFields = append(selectFields,
			"l.ledger_id", "l.name", "l.created_at")
	}

	// Construct the query
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(strings.Join(selectFields, ", "))
	queryBuilder.WriteString(`
        FROM (
            SELECT * FROM blnk.balances WHERE balance_id = $1
        ) AS b
    `)

	if contains(include, "identity") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.identity i ON b.identity_id = i.identity_id
        `)
	}
	if contains(include, "ledger") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.ledgers l ON b.ledger_id = l.ledger_id
        `)
	}

	return queryBuilder.String()
}

func scanRow(row *sql.Row, tx *sql.Tx, include []string) (*model.Balance, error) {
	balance := &model.Balance{}
	identity := &model.Identity{}
	ledger := &model.Ledger{}
	metaDataJSON := []byte{}
	var scanArgs []interface{}
	// Add scan arguments for default fields
	scanArgs = append(scanArgs, &balance.BalanceID, &balance.Balance, &balance.CreditBalance,
		&balance.DebitBalance, &balance.Currency, &balance.CurrencyMultiplier,
		&balance.LedgerID, &balance.IdentityID, &balance.CreatedAt, &metaDataJSON, &balance.InflightBalance, &balance.InflightCreditBalance, &balance.InflightDebitBalance, &balance.Version)

	if contains(include, "identity") {
		scanArgs = append(scanArgs, &identity.IdentityID, &identity.FirstName, &identity.OrganizationName, &identity.Category, &identity.LastName,
			&identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress,
			&identity.PhoneNumber, &identity.Nationality, &identity.Street, &identity.Country,
			&identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt)
	}

	if contains(include, "ledger") {
		scanArgs = append(scanArgs, &ledger.LedgerID, &ledger.Name, &ledger.CreatedAt)
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		fmt.Println("Errror: ", err)
		_ = tx.Rollback()
		return nil, err
	}

	err = json.Unmarshal(metaDataJSON, &balance.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if contains(include, "identity") {
		balance.Identity = identity
	}
	if contains(include, "ledger") {
		balance.Ledger = ledger
	}

	return balance, nil
}

// CreateBalance inserts a new Balance into the database
func (d Datasource) CreateBalance(balance model.Balance) (model.Balance, error) {
	// convert metadata to JSONB
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return balance, err
	}

	balance.BalanceID = model.GenerateUUIDWithSuffix("bln")
	balance.CreatedAt = time.Now()

	// Replace empty string with null for identity_id
	var identityID interface{} = balance.IdentityID
	if balance.IdentityID == "" {
		identityID = nil
	}

	// Replace empty string with null for indicator
	var indicator interface{} = balance.Indicator
	if balance.Indicator == "" {
		indicator = nil
	}

	// insert into database
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.balances (balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, identity_id, indicator, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11)
	`, balance.BalanceID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, identityID, indicator, balance.CreatedAt, &metaDataJSON)

	return balance, err
}

func (d Datasource) GetBalanceByID(id string, include []string) (*model.Balance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	var queryBuilder strings.Builder
	query := prepareQueries(queryBuilder, include)
	row := tx.QueryRow(query, id)
	balance, err := scanRow(row, tx, include)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle no rows error
			return nil, fmt.Errorf("balance with ID '%s' not found", id)
		} else {
			// Handle other errors
			return nil, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return balance, nil
}

func (d Datasource) GetBalanceByIDLite(id string) (*model.Balance, error) {
	var balance model.Balance
	row := d.Conn.QueryRow(`
	   SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = $1
	`, id)

	err := row.Scan(&balance.BalanceID, &balance.Currency, &balance.CurrencyMultiplier, &balance.LedgerID, &balance.Balance, &balance.CreditBalance,
		&balance.DebitBalance, &balance.InflightBalance, &balance.InflightCreditBalance, &balance.InflightDebitBalance, &balance.CreatedAt, &balance.Version)
	if err != nil {
		logrus.Errorf("balance lite error %v", err)
		if err == sql.ErrNoRows {
			return &model.Balance{}, fmt.Errorf("balance with ID '%s' not found", id)
		} else {
			return nil, err
		}
	}

	return &balance, nil
}

func (d Datasource) GetBalanceByIndicator(indicator, currency string) (*model.Balance, error) {
	var balance model.Balance
	row := d.Conn.QueryRow(`
	   SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE indicator = $1 AND currency = $2 
	`, indicator, currency)

	err := row.Scan(&balance.BalanceID, &balance.Currency, &balance.CurrencyMultiplier, &balance.LedgerID, &balance.Balance, &balance.CreditBalance,
		&balance.DebitBalance, &balance.InflightBalance, &balance.InflightCreditBalance, &balance.InflightDebitBalance, &balance.CreatedAt, &balance.Version)
	if err != nil {
		if err == sql.ErrNoRows {
			return &model.Balance{}, fmt.Errorf("balance with indicator '%s' not found", indicator)
		} else {
			return nil, err
		}
	}

	return &balance, nil
}

// GetAllBalances retrieves all balances from the database
func (d Datasource) GetAllBalances() ([]model.Balance, error) {
	// select all balances from database
	rows, err := d.Conn.Query(`
		SELECT balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
		FROM blnk.balances
		LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logrus.Error(err)
		}
	}(rows)

	// create slice to store balances
	var balances []model.Balance

	// iterate through result set and parse metadata from JSON
	for rows.Next() {
		balance := model.Balance{}
		var metaDataJSON []byte
		err = rows.Scan(
			&balance.BalanceID,
			&balance.Balance,
			&balance.CreditBalance,
			&balance.DebitBalance,
			&balance.Currency,
			&balance.CurrencyMultiplier,
			&balance.LedgerID,
			&balance.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		// convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			return nil, err
		}

		balances = append(balances, balance)
	}

	return balances, nil
}

func (d Datasource) GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error) {
	// select all balances from database
	rows, err := d.Conn.Query(`
		SELECT blnk.get_balances_by_id($1,$2)
	`, sourceId, destinationId)
	if err != nil {

		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logrus.Error(err)
		}
	}(rows)

	// create slice to store balances
	var balances []*model.Balance

	// iterate through result set and parse metadata from JSON
	for rows.Next() {
		balance := model.Balance{}
		var metaDataJSON []byte
		err = rows.Scan(
			&balance.BalanceID,
			&balance.Balance,
			&balance.CreditBalance,
			&balance.DebitBalance,
			&balance.Currency,
			&balance.CurrencyMultiplier,
			&balance.LedgerID,
			&balance.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		// convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			return nil, err
		}

		balances = append(balances, &balance)
	}

	return balances, nil
}

func (d Datasource) UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return err
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		return err
	}

	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func updateBalance(ctx context.Context, tx *sql.Tx, balance *model.Balance) error {
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return err
	}

	query := `
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
        WHERE balance_id = $1 AND version = $13
    `

	// Execute the update within the transaction
	result, err := tx.ExecContext(ctx, query, balance.BalanceID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.InflightBalance, balance.InflightCreditBalance, balance.InflightDebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON, balance.Version)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return fmt.Errorf("optimistic locking failure: balance with ID '%s' may have been updated or deleted by another transaction", balance.BalanceID)
	}

	balance.Version++

	return nil
}

// UpdateBalance updates a balance in the database
func (d Datasource) UpdateBalance(balance *model.Balance) error {
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return err
	}

	_, err = d.Conn.Exec(`
		UPDATE blnk.balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`, balance.BalanceID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON)

	return err
}

func (d Datasource) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	monitor.MonitorID = model.GenerateUUIDWithSuffix("mon")
	monitor.CreatedAt = time.Now()

	_, err := d.Conn.Exec(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value,precision,precise_value, description, call_back_url, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7,$8,$9,$10)
	`, monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Condition.Precision, monitor.Condition.PreciseValue, monitor.Description, monitor.CallBackURL, monitor.CreatedAt)

	if err != nil {
		return monitor, err
	}
	return monitor, err
}

func (d Datasource) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	row := d.Conn.QueryRow(`
		SELECT monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at 
		FROM blnk.balance_monitors WHERE monitor_id = $1
	`, id)

	monitor := &model.BalanceMonitor{}
	condition := &model.AlertCondition{}
	err := row.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &condition.Precision, &condition.PreciseValue, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
	if err != nil {
		return nil, err
	}
	monitor.Condition = *condition
	return monitor, nil
}

func (d Datasource) GetAllMonitors() ([]model.BalanceMonitor, error) {
	rows, err := d.Conn.Query(`
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at 
		FROM blnk.balance_monitors
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var monitors []model.BalanceMonitor
	for rows.Next() {
		monitor := model.BalanceMonitor{}
		condition := model.AlertCondition{}
		err = rows.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
		if err != nil {
			return nil, err
		}
		monitor.Condition = condition
		monitors = append(monitors, monitor)
	}
	return monitors, nil
}

func (d Datasource) GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error) {
	rows, err := d.Conn.Query(`
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at 
		FROM blnk.balance_monitors WHERE balance_id= $1
	`, balanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var monitors []model.BalanceMonitor
	for rows.Next() {
		monitor := model.BalanceMonitor{}
		condition := model.AlertCondition{}
		err = rows.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
		if err != nil {
			return nil, err
		}
		monitor.Condition = condition
		monitors = append(monitors, monitor)
	}
	return monitors, nil
}

func (d Datasource) UpdateMonitor(monitor *model.BalanceMonitor) error {
	_, err := d.Conn.Exec(`
		UPDATE blnk.balance_monitors
		SET balance_id = $2, field = $3, operator = $4, value = $5, description = $6, call_back_url= $7
		WHERE monitor_id = $1
	`, monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CallBackURL)
	return err
}

func (d Datasource) DeleteMonitor(id string) error {
	_, err := d.Conn.Exec(`
		DELETE FROM blnk.balance_monitors WHERE monitor_id = $1
	`, id)
	return err
}
