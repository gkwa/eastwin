package eastwin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jessevdk/go-flags"
)

// DeletedTables stores the names of deleted tables
var DeletedTables []string

var opts struct {
	LogFormat string   `long:"log-format" choice:"text" choice:"json" default:"text" required:"false"`
	Verbose   []bool   `short:"v" long:"verbose" description:"Show verbose debug information, each -v bumps log level"`
	Region    string   `short:"r" long:"region" description:"AWS Region" required:"true"`
	Filter    []string `short:"f" long:"filter" description:"Filter tables by substring"`
	Delete    bool     `short:"d" long:"delete" description:"Delete tables"`
	DryRun    bool     `long:"dry-run" description:"Dry run: report what would be deleted"`
	logLevel  slog.Level
}

func Execute() int {
	if err := parseFlags(); err != nil {
		return 1
	}

	if err := setLogLevel(); err != nil {
		return 1
	}

	if err := setupLogger(); err != nil {
		return 1
	}

	if err := run(); err != nil {
		slog.Error("run failed", "error", err)
		return 1
	}

	return 0
}

func parseFlags() error {
	_, err := flags.Parse(&opts)
	return err
}

func run() error {
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		parser.WriteHelp(os.Stdout)
		return fmt.Errorf("error parsing flags: %w", err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(opts.Region))
	if err != nil {
		return fmt.Errorf("error loading AWS configuration: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	tables, err := listTables(client, opts.Filter)
	if err != nil {
		return fmt.Errorf("error listing tables: %w", err)
	}

	if opts.DryRun {
		for _, table := range tables {
			fmt.Printf("dry-run. table would be deleted: %s\n", table)
		}
		return nil
	}

	filteredTables := filterTables(tables, opts.Filter)
	if opts.Delete {
		DeletedTables, err = deleteTables(client, filteredTables)
		if err != nil {
			return fmt.Errorf("error deleting tables: %w", err)
		}

		// Wait until tables are deleted
		err = waitForDeletion(client, DeletedTables)
		if err != nil {
			return fmt.Errorf("error waiting for table deletion: %w", err)
		}

		if err := showAllTables(client); err != nil {
			return fmt.Errorf("error showing remaining tables: %w", err)
		}
	} else {
		for _, table := range filteredTables {
			fmt.Println(table)
		}
	}

	return nil
}

func listTables(client *dynamodb.Client, filter []string) ([]string, error) {
	result, err := client.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}

	var tables []string
	for _, tableName := range result.TableNames {
		table := tableName
		if len(filter) == 0 {
			tables = append(tables, table)
		} else {
			for _, f := range filter {
				if strings.Contains(strings.ToLower(table), strings.ToLower(f)) {
					tables = append(tables, table)
					break
				}
			}
		}
	}

	return tables, nil
}

func filterTables(tables []string, filters []string) []string {
	if len(filters) == 0 {
		return tables
	}

	var filtered []string
	for _, table := range tables {
		for _, filter := range filters {
			if strings.Contains(strings.ToLower(table), strings.ToLower(filter)) {
				filtered = append(filtered, table)
				break
			}
		}
	}

	return filtered
}

func deleteTables(client *dynamodb.Client, tables []string) ([]string, error) {
	var deletedTables []string
	for _, table := range tables {
		tableName := table // Create a new variable to store the value
		_, err := client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
			TableName: &tableName,
		})
		if err != nil {
			return deletedTables, err
		}
		deletedTables = append(deletedTables, table)
	}

	return deletedTables, nil
}

func showAllTables(client *dynamodb.Client) error {
	tables, err := listTables(client, nil)
	if err != nil {
		fmt.Println("Error listing tables:", err)
		return err
	}

	if len(tables) == 0 {
		fmt.Println("no remaining tables.")
	} else {
		fmt.Println("remaining tables:")
		for _, table := range tables {
			fmt.Println(table)
		}
	}

	return nil
}

func waitForDeletion(client *dynamodb.Client, tables []string) error {
	const maxAttempts = 10
	const sleepDuration = time.Duration(1000/6) * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		tablesExist, err := checkTablesExist(client, tables)
		if err != nil {
			return err
		}

		if !tablesExist {
			return nil
		}

		time.Sleep(sleepDuration)
	}

	return fmt.Errorf("tables still exist after waiting for deletion")
}

func checkTablesExist(client *dynamodb.Client, tables []string) (bool, error) {
	existingTables, err := listTables(client, nil)
	if err != nil {
		return false, err
	}

	for _, table := range tables {
		for _, existingTable := range existingTables {
			if table == existingTable {
				return true, nil
			}
		}
	}

	return false, nil
}
