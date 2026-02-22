# Cloud Log Analyzer

A Python CLI tool for aggregating and analyzing logs from AWS CloudWatch, Azure Monitor, and GCP Cloud Logging with Elasticsearch backend for storage and analysis.

## Features

- **Multi-cloud support**: Fetch logs from AWS, Azure, and GCP
- **Elasticsearch integration**: Store and index logs for fast searching
- **CLI interface**: Simple command-line tool for log collection and querying
- **Log aggregation**: Normalize logs from different cloud providers
- **Built-in analytics**: Generate insights and statistics from log data

## Architecture

```
Cloud Providers (AWS/Azure/GCP) → Log Collectors → Elasticsearch → CLI/Analysis
```

## Prerequisites

- Python 3.8+
- Elasticsearch 7.x or 8.x running locally or remotely
- Cloud provider credentials configured:
  - AWS: `~/.aws/credentials` or environment variables
  - Azure: `az login` or service principal
  - GCP: `GOOGLE_APPLICATION_CREDENTIALS` environment variable

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/cloud-log-analyzer.git
cd cloud-log-analyzer

# Install dependencies
pip install -r requirements.txt

# Configure Elasticsearch connection
export ELASTICSEARCH_HOST="localhost:9200"
```

## Usage

### Collect logs from cloud providers

```bash
# Fetch AWS CloudWatch logs
python -m cloud_log_analyzer collect aws --log-group /aws/lambda/my-function --hours 24

# Fetch Azure Monitor logs
python -m cloud_log_analyzer collect azure --workspace-id <id> --hours 24

# Fetch GCP Cloud Logging logs
python -m cloud_log_analyzer collect gcp --project my-project --hours 24
```

### Query and analyze logs

```bash
# Search logs
python -m cloud_log_analyzer query --text "error" --last 1h

# Generate statistics
python -m cloud_log_analyzer stats --group-by level --last 24h

# Show error trends
python -m cloud_log_analyzer analyze --type errors --last 7d
```

### Example Output

```
Log Statistics (Last 24 hours)
================================
Total Logs: 15,234
Errors: 127 (0.83%)
Warnings: 891 (5.85%)
Info: 14,216 (93.32%)

Top Error Messages:
1. Connection timeout (45 occurrences)
2. Database query failed (23 occurrences)
3. Authentication error (18 occurrences)
```

## Configuration

Create a `config.yaml` file:

```yaml
elasticsearch:
  host: localhost:9200
  index_prefix: cloud-logs

aws:
  regions: [us-east-1, us-west-2]

azure:
  subscription_id: your-subscription-id

gcp:
  projects: [project-1, project-2]
```

## Project Structure

```
cloud_log_analyzer/
├── __init__.py
├── __main__.py          # CLI entry point
├── collectors.py        # Cloud provider log collectors
├── storage.py          # Elasticsearch operations
└── analyzer.py         # Log analysis and statistics
```

## License

MIT