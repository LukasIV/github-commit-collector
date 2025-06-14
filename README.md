# GitHub Commit Data Collector

A comprehensive data engineering solution for collecting GitHub commit data to support Next Edit Prediction model training.

## Architecture

```
GitHub API → Data Collection → Transformation → S3 Storage → ML Queries
     ↓              ↓              ↓            ↓           ↓
  Rate Limiting  Schema Mapping  Partitioning  Indexing   Analytics
     ↓              ↓              ↓            ↓           ↓
  Error Handling Data Validation  Compression  Metadata   Filtering
```

## 📁 Project Structure

```
├── github-commit-collector/   # Data collection system
│   ├── batch_collector.py     # Multi-repository collection
│   ├── collector.py           # Main collection logic
│   ├── demo_pipeline.py       # Demo with mock storage
│   ├── docker-compose.yml     # MinIO deployment
│   ├── query_examples.py      # Example queries
│   ├── schema.md              # Data schema documentation
│   ├── setup_storage.sh       # Setup storage script
│   ├── storage_backend.py     # S3/MinIO integration
│   ├── github_commit_dag.py   # Airflow DAG
│   └── README.md              # System docs

```

## Configuration

### Environment Variables

- `GITHUB_TOKEN`: Your GitHub Personal Access Token (required)
- `TARGET_REPOSITORIES`: Comma-separated list of repositories in format "owner/repo"
- `MAX_COMMITS_PER_REPO`: Maximum number of commits to collect per repository (default: 100)
- `OUTPUT_DIR`: Directory to save collected data (default: ./output)

### Example Configuration

```bash
export GITHUB_TOKEN="your_github_token_here"
export TARGET_REPOSITORIES="octocat/Hello-World,JetBrains/clion-debugger-plugin-stub,JetBrains/artifacts-caching-proxy"
export MAX_COMMITS_PER_REPO=100
export OUTPUT_DIR="./output"
```

## Usage Examples

### 1. Data Collection
```bash
export GITHUB_TOKEN="your_token_here"
```

### 2. Storage Pipeline (requires Docker)
```bash
./setup_storage.sh  # Starts MinIO
python3 demo_pipeline.py
```

### 3. Data Analysis with query_examples.py
```bash
python3 query_examples.py
```

The script will perform several analyses:
- Author Productivity: Shows top contributors by commit count and lines changed
- Temporal Patterns: Visualizes commit frequency by hour and day of week
- Commit Message Analysis: Identifies common commit message patterns
- Change Patterns: Analyzes file change statistics


## Rate Limiting

The GitHub API has rate limits:
- 5,000 requests per hour for authenticated requests
- 60 requests per hour for unauthenticated requests

The collector implements automatic rate limiting and retry logic to handle these limits gracefully.

## Output Format

The collector saves data in JSON format with the following structure:

```
output/
├── owner_repo/
│   ├── repository.json
│   ├── commits.json
│   ├── file_changes.json
│   └── authors.json
```

Each file contains structured data according to the schema defined in `schema.md`.

The storage_backend service saves data in Parquet format with the following structure:

```
s3://bucket/
├── repositories_metadata/repository_id=*/repository.parquet
├── commits_metadata/repository_id=*/year=*/month=*/commits.parquet  
├── file_changes_metadata/repository_id=*/file_changes.parquet
├── authors_metadata/authors.parquet
├── file_blobs/{blob_hash}
└── file_patches/{commit_hash}/{file_path}.patch
```