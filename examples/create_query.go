package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	yq "github.com/business-copilot/yandex-query-go"
)

func main() {
	token := os.Getenv("YQ_TOKEN")
	if token == "" {
		log.Fatal("YQ_TOKEN environment variable is not set")
	}

	config := yq.ClientConfig{
		Token:   token,
		Project: "your-project-id", // Replace with your project ID
	}

	client := yq.NewClient(config)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	queryText := `
  SELECT 
   user_id, 
   COUNT(*) as order_count
  FROM 
   orders
  GROUP BY 
   user_id
  ORDER BY 
   order_count DESC
  LIMIT 10
 `

	queryID, err := client.CreateQuery(ctx, queryText, yq.AnalyticsQueryType, "Top 10 users by order count", "Example query", "", "")
	if err != nil {
		log.Fatalf("Failed to create query: %v", err)
	}

	fmt.Printf("Query created with ID: %s\n", queryID)

	resultSetCount, err := client.WaitQueryToSucceed(ctx, queryID, 3*time.Minute, true)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Query completed successfully with %d result sets\n", resultSetCount)

	results, err := client.GetQueryAllResultSets(ctx, queryID, resultSetCount, false)
	if err != nil {
		log.Fatalf("Failed to get query results: %v", err)
	}

	fmt.Println("Query results:")
	resultSet, ok := results.(map[string]interface{})
	if !ok {
		log.Fatal("Unexpected result format")
	}

	columns := resultSet["columns"].([]interface{})
	rows := resultSet["rows"].([][]interface{})

	for _, col := range columns {
		fmt.Printf("%-20s", col.(map[string]interface{})["name"])
	}
	fmt.Println()

	// Print data rows
	for _, row := range rows {
		for _, cell := range row {
			fmt.Printf("%-20v", cell)
		}
		fmt.Println()
	}

	// Get and print the web interface link for the query
	webLink := client.ComposeQueryWebLink(queryID)
	fmt.Printf("\nView query in web interface: %s\n", webLink)
}
