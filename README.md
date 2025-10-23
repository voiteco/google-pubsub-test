# Google Pub/Sub Test Application

A command-line application for testing high-throughput message publishing to Google Cloud Pub/Sub.

## Features

- Fast publishing of large volumes of messages to Pub/Sub
- Flexible configuration via YAML file
- Support for custom message attributes
- Parallel publishing with configurable concurrency level
- Detailed publishing statistics
- Simple CLI interface

## Requirements

- Go 1.21 or higher
- Google Cloud Project with Pub/Sub API enabled
- Configured Google Cloud authentication (Application Default Credentials or service account key)

## Installation

### 1. Clone and Build

```bash
# Navigate to project directory
cd google-pubsub-test

# Download dependencies
go mod download

# Build the application
go build -o pubsub-test
```

### 2. Configure Google Cloud Authentication

**Option A: Application Default Credentials (recommended for local development)**

```bash
gcloud auth application-default login
```

**Option B: Service Account Key**

1. Create a service account in GCP Console
2. Grant it `Pub/Sub Publisher` role
3. Create and download JSON key
4. Specify the key path in `config.yaml` in the `credentialsFile` field

## Configuration

Open the `config.yaml` file and configure the parameters:

```yaml
pubsub:
  # Specify your GCP Project ID
  projectId: "your-gcp-project-id"

  # Specify topic name
  topicId: "your-topic-name"

  # (Optional) Path to service account key
  credentialsFile: ""

message:
  # Directory containing message files
  sourceDir: "./messages"

  # Attributes to be attached to each message
  attributes:
    environment: "test"
    version: "1.0"
    source: "pubsub-test-app"

publishing:
  # Number of concurrent goroutines
  concurrency: 10

  # Batch size for publishing
  batchSize: 100

  # Enable verbose logging
  verbose: true
```

## Preparing Messages

1. Create a file with message content in the `messages/` directory
2. The application will automatically find and read the first file in this directory
3. File format can be any (JSON, XML, plain text, etc.)

Example file is already created: `messages/example-message.json`

## Usage

### Basic Usage

Send 1 message (default):
```bash
./pubsub-test
```

Send 100 messages:
```bash
./pubsub-test -count 100
```

Send 10000 messages:
```bash
./pubsub-test -count 10000
```

### Additional Parameters

Use a different configuration file:
```bash
./pubsub-test -config custom-config.yaml -count 1000
```

### Command Examples

```bash
# Test with a small number of messages
./pubsub-test -count 10

# Load testing
./pubsub-test -count 100000

# With custom config
./pubsub-test -config prod-config.yaml -count 5000
```

## Program Output

After execution, the program displays detailed statistics:

```
2025/10/23 10:00:00 Loaded message from ./messages (156 bytes)
2025/10/23 10:00:00 Target topic: your-project/your-topic
2025/10/23 10:00:00 Starting to publish 1000 messages...
2025/10/23 10:00:01 Published 100 messages
2025/10/23 10:00:02 Published 200 messages
...

=== Publishing Statistics ===
Total messages sent: 1000
Successful: 1000
Failed: 0
Duration: 2.5s
Throughput: 400.00 messages/second

2025/10/23 10:00:02 Done!
```

## Performance Optimization

For maximum publishing speed, configure in `config.yaml`:

```yaml
publishing:
  concurrency: 50      # Increase for greater parallelism
  batchSize: 500       # Increase batch size
  verbose: false       # Disable verbose logging
```

## Project Structure

```
.
├── main.go                      # Main application code
├── config.yaml                  # Configuration file
├── messages/                    # Directory with messages
│   └── example-message.json    # Example message
├── go.mod                       # Go module
├── go.sum                       # Dependencies
└── README.md                    # Documentation
```

## Troubleshooting

### Error "topic does not exist"

Make sure that:
1. The topic exists in your GCP project
2. Project ID and Topic ID are specified correctly in config.yaml

You can create a topic with the command:
```bash
gcloud pubsub topics create your-topic-name
```

### Authentication Errors

Check your authentication setup:
```bash
gcloud auth application-default login
# or
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

### Low Performance

1. Increase `concurrency` in configuration
2. Increase `batchSize` in configuration
3. Check quota limits in GCP Console

## License

MIT
