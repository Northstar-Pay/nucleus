package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"time"

	"github.com/northstar-pay/nucleus/config"

	"github.com/hibiken/asynq"
	"github.com/northstar-pay/nucleus/model"
)

const TRANSACTION_QUEUE = "new:transaction"
const WEBHOOK_QUEUE = "new:webhoook"
const EXPIREDINFLIGHT_QUEUE = "new:inflight-expiry"
const NumberOfQueues = 20

type Queue struct {
	Client    *asynq.Client
	Inspector *asynq.Inspector
}

type TransactionTypePayload struct {
	Data model.Transaction
}

func NewQueue(conf *config.Configuration) *Queue {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	return &Queue{
		Client:    client,
		Inspector: inspector,
	}
}
func (q *Queue) queueInflightExpiry(transactionID string, expiresAt time.Time) error {
	IPayload, err := json.Marshal(transactionID)
	if err != nil {
		log.Fatal(err)
	}
	taskOptions := []asynq.Option{asynq.TaskID(transactionID), asynq.Queue(EXPIREDINFLIGHT_QUEUE), asynq.ProcessIn(time.Until(expiresAt))}
	task := asynq.NewTask(EXPIREDINFLIGHT_QUEUE, IPayload, taskOptions...)
	info, err := q.Client.Enqueue(task)
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued inflight expiry: %+v", transactionID)
	return nil
}

func (q *Queue) Enqueue(_ context.Context, transaction *model.Transaction) error {
	payload, err := json.Marshal(transaction)
	if err != nil {
		log.Fatal(err)
	}
	info, err := q.Client.Enqueue(q.geTask(transaction, payload), asynq.MaxRetry(5))
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued transaction: %+v", transaction.Reference)

	if !transaction.InflightExpiryDate.IsZero() {
		fmt.Println(transaction.InflightExpiryDate)
		return q.queueInflightExpiry(transaction.TransactionID, transaction.InflightExpiryDate)
	}

	return nil
}

func (q *Queue) geTask(transaction *model.Transaction, payload []byte) *asynq.Task {
	// Hash the balance ID and use modulo to select a queue
	queueIndex := hashBalanceID(transaction.Source) % NumberOfQueues
	queueName := fmt.Sprintf("%s_%d", TRANSACTION_QUEUE, queueIndex+1) // Queue names are 1-based

	taskOptions := []asynq.Option{asynq.TaskID(transaction.Reference), asynq.Queue(queueName)}

	if !transaction.ScheduledFor.IsZero() {
		taskOptions = append(taskOptions, asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
	}
	return asynq.NewTask(queueName, payload, taskOptions...)
}

// hashBalanceID returns a consistent hash value for an string balance ID
func hashBalanceID(balanceID string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(balanceID))
	return int(hasher.Sum32())
}
