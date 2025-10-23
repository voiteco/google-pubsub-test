package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
)

// Config structure for application configuration
type Config struct {
	PubSub struct {
		ProjectID       string `yaml:"projectId"`
		TopicID         string `yaml:"topicId"`
		CredentialsFile string `yaml:"credentialsFile"`
	} `yaml:"pubsub"`
	Message struct {
		SourceDir  string            `yaml:"sourceDir"`
		Attributes map[string]string `yaml:"attributes"`
	} `yaml:"message"`
	Publishing struct {
		Concurrency int  `yaml:"concurrency"`
		BatchSize   int  `yaml:"batchSize"`
		Verbose     bool `yaml:"verbose"`
	} `yaml:"publishing"`
}

// loadConfig loads configuration from YAML file
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// loadMessage loads message content from file
func loadMessage(dir string) ([]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read message directory: %w", err)
	}

	// Find the first file in the directory (not a directory)
	for _, entry := range entries {
		if !entry.IsDir() {
			filePath := filepath.Join(dir, entry.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read message file %s: %w", filePath, err)
			}
			return data, nil
		}
	}

	return nil, fmt.Errorf("no message files found in %s", dir)
}

// publishMessages sends a specified number of messages to Pub/Sub
func publishMessages(ctx context.Context, config *Config, messageData []byte, count int) error {
	// Create Pub/Sub client
	var opts []option.ClientOption
	if config.PubSub.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.PubSub.CredentialsFile))
	}

	client, err := pubsub.NewClient(ctx, config.PubSub.ProjectID, opts...)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client: %w", err)
	}
	defer client.Close()

	topic := client.Topic(config.PubSub.TopicID)
	defer topic.Stop()

	// Check if topic exists
	exists, err := topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("topic %s does not exist", config.PubSub.TopicID)
	}

	// Configure publishing settings for maximum throughput
	topic.PublishSettings = pubsub.PublishSettings{
		CountThreshold: config.Publishing.BatchSize,
		DelayThreshold: 10 * time.Millisecond,
		NumGoroutines:  config.Publishing.Concurrency,
	}

	startTime := time.Now()
	var successCount, errorCount int64
	var wg sync.WaitGroup

	// Channel for concurrency control
	semaphore := make(chan struct{}, config.Publishing.Concurrency)

	log.Printf("Starting to publish %d messages...", count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire slot

		go func(index int) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot

			msg := &pubsub.Message{
				Data:       messageData,
				Attributes: config.Message.Attributes,
			}

			result := topic.Publish(ctx, msg)

			// Wait for publish result
			_, err := result.Get(ctx)
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				if config.Publishing.Verbose {
					log.Printf("Error publishing message %d: %v", index, err)
				}
			} else {
				atomic.AddInt64(&successCount, 1)
				if config.Publishing.Verbose && (index+1)%100 == 0 {
					log.Printf("Published %d messages", index+1)
				}
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()

	duration := time.Since(startTime)
	messagesPerSecond := float64(successCount) / duration.Seconds()

	// Print statistics
	fmt.Println("\n=== Publishing Statistics ===")
	fmt.Printf("Total messages sent: %d\n", count)
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", errorCount)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Throughput: %.2f messages/second\n", messagesPerSecond)

	if errorCount > 0 {
		return fmt.Errorf("failed to publish %d messages", errorCount)
	}

	return nil
}

func main() {
	// Define command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	count := flag.Int("count", 1, "Number of messages to send")
	flag.Parse()

	if *count <= 0 {
		log.Fatal("Count must be greater than 0")
	}

	// Load configuration
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Load message
	messageData, err := loadMessage(config.Message.SourceDir)
	if err != nil {
		log.Fatalf("Failed to load message: %v", err)
	}

	log.Printf("Loaded message from %s (%d bytes)", config.Message.SourceDir, len(messageData))
	log.Printf("Target topic: %s/%s", config.PubSub.ProjectID, config.PubSub.TopicID)

	// Publish messages
	ctx := context.Background()
	if err := publishMessages(ctx, config, messageData, *count); err != nil {
		log.Fatalf("Failed to publish messages: %v", err)
	}

	log.Println("Done!")
}
