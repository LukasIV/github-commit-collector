import pandas as pd
import io
from storage_backend import DataPipeline


def example_queries():
    """Demonstrate queries"""
    
    storage_config = {
        'endpoint': 'localhost:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin',
        'bucket_name': 'commit-data',
        'secure': False
    }
    
    pipeline = DataPipeline(storage_config)
    
    print("=== Queries ===\n")
    
    repository_id = "https://github.com/JetBrains/clion-debugger-plugin-stub"
    
    try:
        # Get commits and file changes
        commits_df = pipeline.query_commits_by_repository(repository_id)
        
        if commits_df.empty:
            print("No data available for ML queries")
            return
        
        
        # Query 1: Author productivity analysis
        print("1. Author productivity analysis...")
        author_stats = commits_df.groupby('author_id').agg({
            'commit_hash': 'count',
            'stats_lines_added': 'sum',
            'stats_lines_deleted': 'sum',
            'stats_files_changed': 'sum'
        }).rename(columns={'commit_hash': 'commit_count'})
        
        print("Top authors by commit count:")
        print(author_stats.sort_values('commit_count', ascending=False).head())
        print()
        
        # Query 2: Temporal patterns
        print("2. Temporal commit patterns...")
        commits_df['authored_timestamp'] = pd.to_datetime(commits_df['authored_timestamp'])
        commits_df['hour'] = commits_df['authored_timestamp'].dt.hour
        commits_df['day_of_week'] = commits_df['authored_timestamp'].dt.day_name()
        
        hourly_commits = commits_df['hour'].value_counts().sort_index()
        print("Commits by hour of day:")
        print(hourly_commits.head(10))
        
    except Exception as e:
        print(f"Queries failed: {e}")


if __name__ == "__main__":
    print("\n" + "="*50 + "\n")
    example_queries()

