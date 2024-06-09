package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/northstar-pay/nucleus/config"

	"github.com/northstar-pay/nucleus/model"

	"github.com/hibiken/asynq"
)

func (b *blnkInstance) processTransaction(cxt context.Context, t *asynq.Task) error {
	var txn model.Transaction
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		logrus.Error(err)
		return err
	}
	_, err := b.blnk.RecordTransaction(cxt, &txn)

	if err != nil {
		return err
	}
	//todo only reject insufficient balance transaction and push the rest back for retry

	if err != nil {
		_, err := b.blnk.RejectTransaction(cxt, &txn, err.Error())
		if err != nil {
			return err
		}
		err = blnk.SendWebhook(blnk.NewWebhook{
			Event:   "transaction.rejected",
			Payload: txn,
		})
		return err
	}
	return nil
}

func (b *blnkInstance) procesInflightExpiry(cxt context.Context, t *asynq.Task) error {
	var txnID string
	if err := json.Unmarshal(t.Payload(), &txnID); err != nil {
		logrus.Error(err)
		return err
	}

	_, err := b.blnk.VoidInflightTransaction(cxt, txnID)
	if err != nil {
		return err
	}

	logrus.Printf(" [*] Inflight Transaction Expired %s", txnID)
	return nil
}

func workerCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "start blnk workers",
		Run: func(cmd *cobra.Command, args []string) {
			conf, err := config.Fetch()
			if err != nil {
				log.Println("Error fetching config:", err)
				return
			}

			queues := make(map[string]int)
			queues[blnk.WEBHOOK_QUEUE] = 3
			queues[blnk.EXPIREDINFLIGHT_QUEUE] = 3

			for i := 1; i <= blnk.NumberOfQueues; i++ {
				queueName := fmt.Sprintf("%s_%d", blnk.TRANSACTION_QUEUE, i)
				queues[queueName] = 1
			}

			srv := asynq.NewServer(
				asynq.RedisClientOpt{Addr: conf.Redis.Dns},
				asynq.Config{
					Concurrency: 1,
					Queues:      queues,
				},
			)

			mux := asynq.NewServeMux()
			// Register handler for each queue
			for i := 1; i <= blnk.NumberOfQueues; i++ {
				queueName := fmt.Sprintf("%s_%d", blnk.TRANSACTION_QUEUE, i)
				mux.HandleFunc(queueName, b.processTransaction)
			}

			mux.HandleFunc(blnk.WEBHOOK_QUEUE, blnk.ProcessWebhook)
			mux.HandleFunc(blnk.EXPIREDINFLIGHT_QUEUE, b.procesInflightExpiry)
			if err := srv.Run(mux); err != nil {
				log.Fatal("Error running server:", err)
			}
		},
	}

	return cmd
}
