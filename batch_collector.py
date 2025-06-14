import os
import sys
import logging
from typing import List, Tuple
from collector import CommitDataCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_repositories(repo_string: str) -> List[Tuple[str, str]]:
    """Parse repository string into list of (owner, repo) tuples."""
    repositories = []
    for repo in repo_string.split(','):
        repo = repo.strip()
        if '/' in repo:
            owner, repo_name = repo.split('/', 1)
            repositories.append((owner.strip(), repo_name.strip()))
        else:
            logger.warning(f"Invalid repository format: {repo}. Expected 'owner/repo'")
    return repositories


def main():
    """Main function for batch collection."""
    # Get configuration from environment variables
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        logger.error("GITHUB_TOKEN environment variable is required")
        sys.exit(1)
    
    target_repos = os.getenv('TARGET_REPOSITORIES')
    max_commits = int(os.getenv('MAX_COMMITS_PER_REPO', '100'))
    output_base_dir = os.getenv('OUTPUT_DIR', './output')
    
    # Parse repositories
    repositories = parse_repositories(target_repos)
    if not repositories:
        logger.error("No valid repositories found in TARGET_REPOSITORIES")
        sys.exit(1)
    
    logger.info(f"Starting batch collection for {len(repositories)} repositories")
    logger.info(f"Max commits per repo: {max_commits}")
    logger.info(f"Output directory: {output_base_dir}")
    
    # Initialize collector
    collector = CommitDataCollector(github_token)
    
    successful_collections = 0
    failed_collections = 0
    
    for owner, repo_name in repositories:
        try:
            logger.info(f"Collecting data for {owner}/{repo_name}")
            
            # Collect data
            data = collector.collect_repository_data(owner, repo_name, max_commits)
            
            # Save data
            output_dir = os.path.join(output_base_dir, f"{owner}_{repo_name}")
            collector.save_to_json(output_dir, data)
            
            successful_collections += 1
            logger.info(f"Successfully collected data for {owner}/{repo_name}")
            
        except Exception as e:
            logger.error(f"Failed to collect data for {owner}/{repo_name}: {e}")
            failed_collections += 1
            continue
    
    logger.info(f"Batch collection completed. Success: {successful_collections}, Failed: {failed_collections}")


if __name__ == "__main__":
    main()

