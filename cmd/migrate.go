package main

import (
	"fmt"
	blnk "github.com/northstar-pay/nucleus"
	"log"

	"github.com/northstar-pay/nucleus/config"
	"github.com/northstar-pay/nucleus/database"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/spf13/cobra"
)

func migrateCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "start blnk migration",
	}

	cmd.AddCommand(migrateUpCommands())
	cmd.AddCommand(migrateDownCommands())

	return cmd
}

func migrateUpCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "up",
		Run: func(cmd *cobra.Command, args []string) {
			migrations := migrate.EmbedFileSystemMigrationSource{
				FileSystem: blnk.SQLFiles,
				Root:       "sql",
			}
			cnf, err := config.Fetch()
			if err != nil {
				return
			}
			db, err := database.ConnectDB(cnf.DataSource.Dns)
			if err != nil {
				return
			}
			migrate.SetSchema("blnk")

			er, _ := migrations.FindMigrations()

			for _, v := range er {
				fmt.Println(v.Id)
			}

			n, err := migrate.Exec(db, "postgres", migrations, migrate.Up)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
			}
			fmt.Printf("Applied %d migrations!\n", n)
		},
	}

	return cmd
}

func migrateDownCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "down",
		Run: func(cmd *cobra.Command, args []string) {
			migrations := migrate.EmbedFileSystemMigrationSource{
				FileSystem: blnk.SQLFiles,
				Root:       "sql",
			}
			cnf, err := config.Fetch()
			if err != nil {
				return
			}
			db, err := database.ConnectDB(cnf.DataSource.Dns)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
				return
			}
			n, err := migrate.Exec(db, "postgres", migrations, migrate.Down)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
			}
			fmt.Printf("Applied %d migrations!\n", n)
		},
	}

	return cmd
}
