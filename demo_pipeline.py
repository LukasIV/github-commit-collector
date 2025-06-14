import sys
import os
import json
from storage_backend import DataPipeline
from batch_collector import main as batch_collect

def main():
    """Demonstrate the complete data pipeline using batch collector."""
    print("=== GitHub Commit Data Pipeline Demo ===\n")
    
    # Configuration
    storage_config = {
        'endpoint': 'localhost:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin',
        'bucket_name': 'commit-data',
        'secure': False
    }
    
    # Initialize pipeline
    print("1. Initializing data pipeline...")
    pipeline = DataPipeline(storage_config)
    print("✓ Pipeline initialized\n")
    
    # Collect sample data (if GitHub token is available)
    github_token = os.getenv('GITHUB_TOKEN')
    if github_token:
        print("2. Collecting sample data from GitHub using batch collector...")
        
        # Set environment variables for batch collector
        os.environ['TARGET_REPOSITORIES'] = 'octocat/Hello-World,JetBrains/clion-debugger-plugin-stub,JetBrains/artifacts-caching-proxy'
        os.environ['MAX_COMMITS_PER_REPO'] = '100000'
        os.environ['OUTPUT_DIR'] = './temp_data'
        
        # Run batch collection
        try:
            batch_collect()
            print("✓ Sample data collected\n")
            
            # Get list of repositories to process
            repositories = os.environ['TARGET_REPOSITORIES'].split(',')
            
            # Process and store data for each collected repository
            for repo in repositories:
                if not repo.strip():
                    continue
                    
                try:
                    owner, repo_name = repo.strip().split('/')
                except ValueError:
                    print(f"⚠️ Invalid repository format: {repo}, skipping...")
                    continue
                    
                repo_dir = f"{owner}_{repo_name}".replace(' ', '_')
                repo_path = os.path.join('./temp_data', repo_dir)
                
                if not os.path.exists(repo_path):
                    print(f"⚠️ Data not found for {repo}, skipping...")
                    continue
                    
                print(f"3. Processing and storing data for {repo}...")
                try:
                    # Process collected data
                    results = pipeline.process_collected_data(repo_path)
                    print(f"✓ Data processed and stored for {repo}")
                    print(f"Storage results: {json.dumps(results, indent=2)}\n")
                    
                    # Get repository ID from the collected data
                    try:
                        repo_json_path = os.path.join(repo_path, 'repository.json')
                        with open(repo_json_path) as f:
                            repo_data = json.load(f)
                            repository_id = repo_data['repository_id']
                            
                        # Demonstrate queries
                        print(f"4. Demonstrating data queries for {repo}...")
                        commits_df = pipeline.query_commits_by_repository(repository_id)
                        print(f"✓ Found {len(commits_df)} commits for repository {repo}")
                        
                        if not commits_df.empty:
                            print("Sample commit data:")
                            print(commits_df[['commit_hash', 'message', 'stats_lines_added', 'stats_lines_deleted']].head())
                    except FileNotFoundError:
                        print(f"⚠️ repository.json not found for {repo}, skipping queries")
                    except Exception as e:
                        print(f"⚠️ Error querying data for {repo}: {e}")
                        
                    print("\n")
                except Exception as e:
                    print(f"⚠️ Failed to process data for {repo}: {e}")
            
            print("✓ Pipeline demo completed successfully for all repositories!")
            
        except Exception as e:
            print(f"Error during data collection: {e}")
            print("Skipping data collection demo...")
    else:
        print("2. No GitHub token provided, skipping data collection demo")
        print("   Set GITHUB_TOKEN environment variable to enable data collection\n")
    
    # Demonstrate storage capabilities without data collection
    print("5. Testing storage backend capabilities...")
    
    # Test basic storage operations
    test_data = b"Hello, World! This is test data."
    test_key = pipeline.storage.upload_object("test/hello.txt", test_data, "text/plain")
    print(f"✓ Uploaded test object: {test_key}")
    
    # List objects
    objects = pipeline.storage.list_objects("test/")
    print(f"✓ Found {len(objects)} objects in test/ prefix")
    
    # Download object
    downloaded_data = pipeline.storage.download_object("test/hello.txt")
    print(f"✓ Downloaded object, size: {len(downloaded_data)} bytes")
    
    print("\n=== Demo completed ===")

if __name__ == "__main__":
    main()