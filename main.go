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

// loadMessages loads all message files from directory
func loadMessages(dir string) ([][]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read message directory: %w", err)
	}

	var messages [][]byte

	// Load all files in the directory (not directories)
	for _, entry := range entries {
		if !entry.IsDir() {
			filePath := filepath.Join(dir, entry.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read message file %s: %w", filePath, err)
			}
			messages = append(messages, data)
		}
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("no message files found in %s", dir)
	}

	return messages, nil
}

// publishMessages sends messages to Pub/Sub based on count parameter
// If count is 0, sends all messages once
// If count <= number of messages, sends first 'count' messages
// If count > number of messages, sends messages cyclically until 'count' is reached
func publishMessages(ctx context.Context, config *Config, messages [][]byte, count int) error {
	if len(messages) == 0 {
		return fmt.Errorf("no messages to publish")
	}

	// Determine how many messages to send
	var messagesToSend int
	if count == 0 {
		// Send all messages once
		messagesToSend = len(messages)
	} else {
		messagesToSend = count
	}

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

	log.Printf("Starting to publish %d messages (from %d unique files)...", messagesToSend, len(messages))

	for i := 0; i < messagesToSend; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire slot

		// Use modulo to cycle through messages if needed
		messageIndex := i % len(messages)

		go func(index int, msgData []byte) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot

			msg := &pubsub.Message{
				Data:       msgData,
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
		}(i, messages[messageIndex])
	}

	// Wait for all operations to complete
	wg.Wait()

	duration := time.Since(startTime)
	messagesPerSecond := float64(successCount) / duration.Seconds()

	// Print statistics
	fmt.Println("\n=== Publishing Statistics ===")
	fmt.Printf("Total messages sent: %d\n", messagesToSend)
	fmt.Printf("Unique message files: %d\n", len(messages))
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
	count := flag.Int("count", 0, "Number of messages to send (0 = send all files once)")
	flag.Parse()

	if *count < 0 {
		log.Fatal("Count must be 0 or greater (0 = send all files)")
	}

	// Load configuration
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Load all messages
	messages, err := loadMessages(config.Message.SourceDir)
	if err != nil {
		log.Fatalf("Failed to load messages: %v", err)
	}

	log.Printf("Loaded %d message files from %s", len(messages), config.Message.SourceDir)
	log.Printf("Target topic: %s/%s", config.PubSub.ProjectID, config.PubSub.TopicID)

	// Publish messages
	ctx := context.Background()
	if err := publishMessages(ctx, config, messages, *count); err != nil {
		log.Fatalf("Failed to publish messages: %v", err)
	}

	log.Println("Done!")
}
