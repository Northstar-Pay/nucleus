package blnk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	redlock "github.com/northstar-pay/nucleus/internal/lock"
	"github.com/northstar-pay/nucleus/internal/notification"
	"go.opentelemetry.io/otel/trace"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	"github.com/northstar-pay/nucleus/model"
)

var (
	tracer = otel.Tracer("Queue transaction")
)

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
	StatusInflight  = "INFLIGHT"
	StatusVoid      = "VOID"
	StatusRejected  = "REJECTED"
)

func getEventFromStatus(status string) string {
	switch strings.ToLower(status) {
	case strings.ToLower(StatusQueued):
		return "transaction.queued"
	case strings.ToLower(StatusApplied):
		return "transaction.applied"
	case strings.ToLower(StatusScheduled):
		return "transaction.scheduled"
	case strings.ToLower(StatusInflight):
		return "transaction.inflight"
	case strings.ToLower(StatusVoid):
		return "transaction.void"
	case strings.ToLower(StatusRejected):
		return "transaction.rejected"
	default:
		return "transaction.unknown"
	}
}

func (l *Blnk) getSourceAndDestination(transaction *model.Transaction) (source *model.Balance, destination *model.Balance, err error) {
	var sourceBalance, destinationBalance *model.Balance

	// Check if Source starts with "@"
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err = l.getOrCreateBalanceByIndicator(transaction.Source, transaction.Currency)
		if err != nil {
			logrus.Errorf("source error %v", err)
			return nil, nil, err
		}
		// Update transaction source with the balance ID
		transaction.Source = sourceBalance.BalanceID
	} else {
		sourceBalance, err = l.datasource.GetBalanceByIDLite(transaction.Source)
		if err != nil {
			logrus.Errorf("source error %v", err)
			return nil, nil, err
		}
	}

	// Check if Destination starts with "@"
	if strings.HasPrefix(transaction.Destination, "@") {
		destinationBalance, err = l.getOrCreateBalanceByIndicator(transaction.Destination, transaction.Currency)
		if err != nil {
			logrus.Errorf("destination error %v", err)
			return nil, nil, err
		}
		// Update transaction destination with the balance ID
		transaction.Destination = destinationBalance.BalanceID
	} else {
		destinationBalance, err = l.datasource.GetBalanceByIDLite(transaction.Destination)
		if err != nil {
			logrus.Errorf("destination error %v", err)
			return nil, nil, err
		}
	}
	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) acquireLock(ctx context.Context, transaction *model.Transaction) (*redlock.Locker, error) {
	locker := redlock.NewLocker(l.redis, transaction.Source, model.GenerateUUIDWithSuffix("loc"))
	err := locker.Lock(ctx, time.Minute*30)
	if err != nil {
		return nil, err
	}
	return locker, nil
}

func (l *Blnk) updateTransactionDetails(transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.Transaction {
	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID
	if transaction.Status == StatusQueued || transaction.Status == StatusScheduled {
		transaction.Status = StatusApplied
	}
	return transaction
}

func (l *Blnk) persistTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		logrus.Errorf("ERROR saving transaction to db. %s", err)
		return nil, err
	}
	return transaction, nil
}

func (l *Blnk) postTransactionActions(_ context.Context, transaction *model.Transaction) {
	go func() {
		err := SendWebhook(NewWebhook{
			Event:   getEventFromStatus(transaction.Status),
			Payload: transaction,
		})
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

func (l *Blnk) updateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	var wg sync.WaitGroup
	if err := l.datasource.UpdateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return err
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(sourceBalance)
	}()
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(destinationBalance)
	}()
	wg.Wait()

	return nil
}

func (l *Blnk) validateTxn(cxt context.Context, transaction *model.Transaction) error {
	cxt, span := tracer.Start(cxt, "Validating transaction reference")
	defer span.End()
	txn, err := l.datasource.TransactionExistsByRef(cxt, transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if txn {
		return fmt.Errorf("reference %s has already been used", transaction.Reference)
	}

	return nil
}

func (l *Blnk) applyTransactionToBalances(span trace.Span, balances []*model.Balance, transaction *model.Transaction) error {
	span.AddEvent("calculating new balances")
	defer span.End()

	err := model.UpdateBalances(transaction, balances[0], balances[1])
	if err != nil {
		return err
	}
	return nil
}

func (l *Blnk) RecordTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Recording transaction")
	defer span.End()

	return l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		sourceBalance, destinationBalance, err := l.validateAndPrepareTransaction(ctx, span, transaction)
		if err != nil {
			return nil, err
		}

		if err := l.processBalances(ctx, span, transaction, sourceBalance, destinationBalance); err != nil {
			return nil, err
		}

		transaction, err = l.finalizeTransaction(ctx, span, transaction, sourceBalance, destinationBalance)
		if err != nil {
			return nil, err
		}

		l.postTransactionActions(ctx, transaction)

		return transaction, nil
	})
}

func (l *Blnk) executeWithLock(ctx context.Context, transaction *model.Transaction, fn func(context.Context) (*model.Transaction, error)) (*model.Transaction, error) {
	locker, err := l.acquireLock(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	return fn(ctx)
}

func (l *Blnk) validateAndPrepareTransaction(ctx context.Context, span trace.Span, transaction *model.Transaction) (*model.Balance, *model.Balance, error) {
	if err := l.validateTxn(ctx, transaction); err != nil {
		return nil, nil, l.logAndRecordError(span, "transaction validation failed", err)
	}

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		return nil, nil, l.logAndRecordError(span, "failed to get source and destination balances", err)
	}

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID

	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) processBalances(ctx context.Context, span trace.Span, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	if err := l.applyTransactionToBalances(span, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		return l.logAndRecordError(span, "failed to apply transaction to balances", err)
	}

	if err := l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return l.logAndRecordError(span, "failed to update balances", err)
	}

	return nil
}

func (l *Blnk) finalizeTransaction(ctx context.Context, span trace.Span, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	transaction = l.updateTransactionDetails(transaction, sourceBalance, destinationBalance)

	transaction, err := l.persistTransaction(ctx, transaction)
	if err != nil {
		return nil, l.logAndRecordError(span, "failed to persist transaction", err)
	}

	return transaction, nil
}

func (l *Blnk) releaseLock(ctx context.Context, locker *redlock.Locker) {
	if err := locker.Unlock(ctx); err != nil {
		logrus.Error("failed to release lock", err)
	}
}

func (l *Blnk) logAndRecordError(span trace.Span, msg string, err error) error {
	span.RecordError(err)
	logrus.Error(msg, err)
	return fmt.Errorf("%s: %w", msg, err)
}

func (l *Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
	transaction.Status = StatusRejected
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}
	transaction.MetaData["blnk_rejection_reason"] = reason

	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		logrus.Errorf("ERROR saving transaction to db. %s", err)
	}

	err = SendWebhook(NewWebhook{
		Event:   "transaction.applied",
		Payload: transaction,
	})
	if err != nil {
		notification.NotifyError(err)
	}

	return transaction, nil
}
func (l *Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount float64) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Committing inflight transaction")
	defer span.End()

	transaction, err := l.fetchAndValidateTransaction(ctx, span, transactionID)
	if err != nil {
		return nil, err
	}

	return l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
		if err != nil {
			return nil, l.logAndRecordError(span, "source and destination balance error", err)
		}

		if err := l.validateAndUpdateAmount(ctx, span, transaction, amount); err != nil {
			return nil, err
		}

		if err := l.commitBalances(ctx, span, transaction, sourceBalance, destinationBalance); err != nil {
			return nil, err
		}

		return l.finalizeCommitment(ctx, span, transaction)
	})
}

func (l *Blnk) fetchAndValidateTransaction(_ context.Context, span trace.Span, transactionID string) (*model.Transaction, error) {
	transaction, err := l.datasource.GetTransaction(transactionID)
	if err != nil {
		return nil, l.logAndRecordError(span, "fetch transaction error", err)
	}

	if transaction.Status != StatusInflight {
		return nil, l.logAndRecordError(span, "invalid transaction status", fmt.Errorf("transaction is not in inflight status"))
	}

	return transaction, nil
}

func (l *Blnk) validateAndUpdateAmount(_ context.Context, span trace.Span, transaction *model.Transaction, amount float64) error {
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(transaction.TransactionID)
	if err != nil {
		return l.logAndRecordError(span, "error fetching committed amount", err)
	}

	originalAmount := transaction.PreciseAmount
	amountLeft := originalAmount - committedAmount

	if amount != 0 {
		transaction.Amount = amount
		transaction.PreciseAmount = 0
	} else {
		transaction.Amount = float64(amountLeft) / transaction.Precision
	}

	if amountLeft < model.ApplyPrecision(transaction) {
		return fmt.Errorf("can not commit %s%.2f. You can only commit an amount between 1.00 - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(amountLeft)/transaction.Precision)
	} else if amountLeft == 0 {
		return fmt.Errorf("can not commit %s%.2f. Transaction already committed with amount of - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(committedAmount)/transaction.Precision)
	}

	return nil
}

func (l *Blnk) commitBalances(ctx context.Context, span trace.Span, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	sourceBalance.CommitInflightDebit(transaction)
	destinationBalance.CommitInflightCredit(transaction)

	if err := l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return l.logAndRecordError(span, "update balances error", err)
	}

	return nil
}

func (l *Blnk) finalizeCommitment(ctx context.Context, span trace.Span, transaction *model.Transaction) (*model.Transaction, error) {
	transaction.Status = StatusApplied
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()

	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	return transaction, nil
}

func (l *Blnk) VoidInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Voiding inflight transaction")
	defer span.End()

	transaction, err := l.fetchAndValidateInflightTransaction(ctx, span, transactionID)
	if err != nil {
		return nil, err
	}

	return l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
		if err != nil {
			return nil, l.logAndRecordError(span, "source and destination balance error", err)
		}

		amountLeft, err := l.calculateRemainingAmount(ctx, span, transaction)
		if err != nil {
			return nil, err
		}

		if err := l.rollbackBalances(ctx, span, transaction, sourceBalance, destinationBalance, amountLeft); err != nil {
			return nil, err
		}

		return l.finalizeVoidTransaction(ctx, span, transaction, amountLeft)
	})
}

func (l *Blnk) fetchAndValidateInflightTransaction(_ context.Context, span trace.Span, transactionID string) (*model.Transaction, error) {
	transaction, err := l.datasource.GetTransaction(transactionID)
	if err != nil {
		return nil, l.logAndRecordError(span, "fetch transaction error", err)
	}

	if transaction.Status != StatusInflight {
		return nil, l.logAndRecordError(span, "invalid transaction status", fmt.Errorf("transaction is not in inflight status"))
	}

	parentVoided, err := l.datasource.IsParentTransactionVoid(transactionID)
	if err != nil {
		return nil, l.logAndRecordError(span, "error checking parent transaction status", err)
	}

	if parentVoided {
		return nil, l.logAndRecordError(span, "transaction already voided", fmt.Errorf("transaction has already been voided"))
	}

	return transaction, nil
}

func (l *Blnk) calculateRemainingAmount(_ context.Context, span trace.Span, transaction *model.Transaction) (int64, error) {
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(transaction.TransactionID)
	if err != nil {
		return 0, l.logAndRecordError(span, "error fetching committed amount", err)
	}

	return transaction.PreciseAmount - committedAmount, nil
}

func (l *Blnk) rollbackBalances(ctx context.Context, span trace.Span, _ *model.Transaction, sourceBalance, destinationBalance *model.Balance, amountLeft int64) error {
	sourceBalance.RollbackInflightDebit(amountLeft)
	destinationBalance.RollbackInflightCredit(amountLeft)

	if err := l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return l.logAndRecordError(span, "update balances error", err)
	}

	return nil
}

func (l *Blnk) finalizeVoidTransaction(ctx context.Context, span trace.Span, transaction *model.Transaction, amountLeft int64) (*model.Transaction, error) {
	transaction.Status = StatusVoid
	transaction.Amount = float64(amountLeft) / transaction.Precision
	transaction.PreciseAmount = amountLeft
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()

	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	return transaction, nil
}

func (l *Blnk) QueueTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Queuing transaction")
	defer span.End()

	if err := l.validateTxn(ctx, transaction); err != nil {
		return nil, err
	}

	setTransactionStatus(transaction)
	setTransactionMetadata(transaction)

	transactions, err := transaction.SplitTransaction()
	if err != nil {
		return nil, err
	}

	if err := enqueueTransactions(ctx, l.queue, transaction, transactions); err != nil {
		return nil, err
	}

	return transaction, nil
}

func setTransactionStatus(transaction *model.Transaction) {
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	} else if transaction.Inflight {
		transaction.Status = StatusInflight
	} else {
		transaction.Status = StatusQueued
	}
}

func setTransactionMetadata(transaction *model.Transaction) {
	transaction.SkipBalanceUpdate = true
	transaction.CreatedAt = time.Now()
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Hash = transaction.HashTxn()
	transaction.PreciseAmount = int64(transaction.Amount * transaction.Precision)
}

func enqueueTransactions(ctx context.Context, queue *Queue, originalTransaction *model.Transaction, splitTransactions []*model.Transaction) error {
	transactionsToEnqueue := splitTransactions
	if len(transactionsToEnqueue) == 0 {
		transactionsToEnqueue = []*model.Transaction{originalTransaction}
	}

	for _, txn := range transactionsToEnqueue {
		if err := queue.Enqueue(ctx, txn); err != nil {
			notification.NotifyError(err)
			logrus.Errorf("Error queuing transaction: %v", err)
			return err
		}
	}

	return nil
}

func (l *Blnk) GetTransaction(TransactionID string) (*model.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l *Blnk) GetAllTransactions() ([]model.Transaction, error) {
	return l.datasource.GetAllTransactions()
}

func (l *Blnk) GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error) {
	return l.datasource.GetTransactionByRef(cxt, reference)
}

func (l *Blnk) UpdateTransactionStatus(id string, status string) error {
	return l.datasource.UpdateTransactionStatus(id, status)
}

func (l *Blnk) RefundTransaction(transactionID string) (*model.Transaction, error) {
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		return &model.Transaction{}, err
	}

	parentVoided, err := l.datasource.IsParentTransactionVoid(transactionID)
	if err != nil {
		return nil, err
	}

	if parentVoided && originalTxn.Status == StatusInflight {
		originalTxn.Inflight = true
	}

	newTransaction := *originalTxn
	newTransaction.Reference = model.GenerateUUIDWithSuffix("ref")
	newTransaction.ParentTransaction = originalTxn.TransactionID
	newTransaction.Source = originalTxn.Destination
	newTransaction.Destination = originalTxn.Source
	newTransaction.AllowOverdraft = true
	refundTxn, err := l.QueueTransaction(context.Background(), &newTransaction)
	if err != nil {
		return &model.Transaction{}, err
	}

	return refundTxn, nil
}
