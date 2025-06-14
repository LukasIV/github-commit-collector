from datetime import datetime, timedelta
import os
import json
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from storage_backend import DataPipeline, Storage
from collector import CommitDataCollector

# Default configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

storage_config = {
    'endpoint': 'localhost:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'bucket_name': 'commit-data',
    'secure': False
}

# DAG configuration
with DAG(
    'github_commit_pipeline',
    default_args=default_args,
    description='Incremental GitHub commit data collection pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['github', 'data-pipeline'],
) as dag:

    @task
    def get_last_collected_timestamps():
        """
        Retrieve the last collected timestamp for each repository from state storage
        """
        storage = Storage(storage_config)
        state_files = storage.list_objects("state/")
        timestamps = {}
        
        for file_key in state_files:
            try:
                # Extract repo name from state file path: state/repo_name/last_timestamp.txt
                repo_name = file_key.split('/')[1]
                content = storage.download_object(file_key)
                timestamps[repo_name] = content.decode('utf-8').strip()
                logging.info(f"Found last timestamp for {repo_name}: {timestamps[repo_name]}")
            except Exception as e:
                logging.error(f"Error reading state file {file_key}: {e}")
        
        return timestamps

    @task
    def collect_commit_data(repo: str, last_timestamp: str = None):
        """
        Collect new commits for a repository since the last collected timestamp
        """
        github_token = os.getenv('GITHUB_TOKEN')
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable not set")
        
        owner, repo_name = repo.split('/')
        repo_dir = f"{owner}_{repo_name}"
        
        logging.info(f"Starting incremental collection for {repo}")
        logging.info(f"Last collected timestamp: {last_timestamp or 'None (first run)'}")
        
        collector = CommitDataCollector(github_token)
        
        # Collect data since last timestamp
        data = collector.collect_repository_data(
            owner, 
            repo_name,
            since=last_timestamp
        )
        
        # Save to temporary directory
        temp_dir = f"/tmp/airflow_data/{repo_dir}"
        os.makedirs(temp_dir, exist_ok=True)
        collector.save_to_json(temp_dir, data)
        
        # Find the latest commit timestamp in this batch
        latest_timestamp = max(
            commit.committed_timestamp for commit in data['commits']
        ) if data['commits'] else last_timestamp
        
        logging.info(f"Collected {len(data['commits']} new commits for {repo}")
        logging.info(f"New latest timestamp: {latest_timestamp}")
        
        return {
            'repo': repo,
            'repo_dir': repo_dir,
            'temp_dir': temp_dir,
            'latest_timestamp': latest_timestamp,
            'repository_id': data['repository'].repository_id
        }

    @task
    def process_and_store_data(collection_result: dict):
        """
        Process collected data and store in persistent storage
        """
        pipeline = DataPipeline(storage_config)
        results = pipeline.process_collected_data(collection_result['temp_dir'])
        
        logging.info(f"Processed data for {collection_result['repo']}:")
        logging.info(json.dumps(results, indent=2))
        
        return {
            'repo': collection_result['repo'],
            'repo_dir': collection_result['repo_dir'],
            'repository_id': collection_result['repository_id']
        }

    @task
    def update_last_timestamp(collection_result: dict, last_timestamps: dict):
        """
        Update the last collected timestamp for the repository
        """
        repo_dir = collection_result['repo_dir']
        latest_timestamp = collection_result['latest_timestamp']
        
        storage = Storage(storage_config)
        state_key = f"state/{repo_dir}/last_timestamp.txt"
        
        # Create state directory if it doesn't exist
        try:
            storage.upload_object(state_key, latest_timestamp.encode('utf-8'), 'text/plain')
            logging.info(f"Updated last timestamp for {repo_dir}: {latest_timestamp}")
        except Exception as e:
            logging.error(f"Failed to update state for {repo_dir}: {e}")
            raise
        
        # Update the in-memory state for downstream tasks
        last_timestamps[repo_dir] = latest_timestamp
        return last_timestamps

    @task
    def demonstrate_data_usage(processing_result: dict):
        """
        Demonstrate data usage by querying the stored commits
        """
        pipeline = DataPipeline(storage_config)
        commits_df = pipeline.query_commits_by_repository(processing_result['repository_id'])
        
        logging.info(f"Found {len(commits_df)} commits for {processing_result['repo']}")
        if not commits_df.empty:
            sample = commits_df[['commit_hash', 'message', 'stats_lines_added', 'stats_lines_deleted']].head()
            logging.info("Sample commit data:")
            logging.info(sample.to_string())
        
        return True

    @task
    def cleanup_temp_files(collection_result: dict):
        """
        Clean up temporary files after processing
        """
        try:
            temp_dir = collection_result['temp_dir']
            # Remove temporary directory
            for root, dirs, files in os.walk(temp_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(temp_dir)
            logging.info(f"Cleaned up temporary files for {collection_result['repo']}")
        except Exception as e:
            logging.error(f"Error cleaning up temporary files: {e}")

    # Define the list of repositories to process
    repositories = [
        'octocat/Hello-World',
        'martinmimigames/tiny-music-player',
        # Add more repositories as needed
    ]

    # Start pipeline
    last_timestamps = get_last_collected_timestamps()
    
    # Process each repository in parallel
    for repo in repositories:
        owner, repo_name = repo.split('/')
        repo_dir = f"{owner}_{repo_name}"
        
        # Get last timestamp for this specific repository
        repo_last_timestamp = last_timestamps.get(repo_dir)
        
        # Collect data
        collection_result = collect_commit_data.override(task_id=f'collect_{repo_dir}')(repo, repo_last_timestamp)
        
        # Process and store
        processing_result = process_and_store_data.override(task_id=f'process_{repo_dir}')(collection_result)
        
        # Update timestamp
        updated_timestamps = update_last_timestamp.override(task_id=f'update_{repo_dir}')(collection_result, last_timestamps)
        
        # Demonstrate usage
        demonstrate_data_usage.override(task_id=f'query_{repo_dir}')(processing_result)
        
        # Cleanup
        cleanup_temp_files.override(task_id=f'cleanup_{repo_dir}')(collection_result)
        
        # Update the last_timestamps for the next repository
        last_timestamps = updated_timestamps

    # Dependencies between the initial state and repository processing
    last_timestamps >> collection_result