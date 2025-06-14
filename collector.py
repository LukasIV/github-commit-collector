import os
import json
import time
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Repository:
    repository_id: str
    name: str
    owner: str
    description: str
    primary_language: str
    clone_url: str
    created_at: str
    last_updated_at: str
    metadata: Dict[str, Any]


@dataclass
class Author:
    author_id: str
    name: str
    username: str
    email: str


@dataclass
class Commit:
    commit_hash: str
    repository_id: str
    author_id: str
    committer_id: str
    message: str
    authored_timestamp: str
    committed_timestamp: str
    parent_hashes: List[str]
    tree_hash: str
    stats_lines_added: int
    stats_lines_deleted: int
    stats_files_changed: int


@dataclass
class FileChange:
    file_change_id: str
    commit_hash: str
    file_path: str
    change_type: str
    old_file_path: Optional[str]
    lines_added: int
    lines_deleted: int
    file_mode_before: Optional[str]
    file_mode_after: Optional[str]
    blob_hash_before: Optional[str]
    blob_hash_after: Optional[str]
    content_before_s3_key: Optional[str]
    content_after_s3_key: Optional[str]
    patch_s3_key: Optional[str]
    file_type: str
    is_binary: bool


class GitHubAPIClient:
    """GitHub API client with rate limiting and error handling."""
    
    def __init__(self, token: str, base_url: str = "https://api.github.com"):
        self.token = token
        self.base_url = base_url
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set headers
        self.session.headers.update({
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHub-Commit-Collector/1.0'
        })
        
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = time.time()
    
    def _check_rate_limit(self):
        """Check and handle rate limiting."""
        if self.rate_limit_remaining <= 10:
            sleep_time = max(0, self.rate_limit_reset - time.time() + 60)
            if sleep_time > 0:
                logger.warning(f"Rate limit low. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Make a request to the GitHub API with rate limiting."""
        self._check_rate_limit()
        
        try:
            response = self.session.get(url, params=params)
            
            # Update rate limit info
            self.rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', time.time()))
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def get_repository(self, owner: str, repo: str) -> Dict:
        """Get repository information."""
        url = f"{self.base_url}/repos/{owner}/{repo}"
        return self._make_request(url)
    
    def get_commits(self, owner: str, repo: str, since: Optional[str] = None, 
                   until: Optional[str] = None, per_page: int = 100) -> List[Dict]:
        """Get commits for a repository."""
        url = f"{self.base_url}/repos/{owner}/{repo}/commits"
        params = {'per_page': per_page}
        
        if since:
            params['since'] = since
        if until:
            params['until'] = until
        
        commits = []
        page = 1
        
        while True:
            params['page'] = page
            response_data = self._make_request(url, params)
            
            if not response_data:
                break
                
            commits.extend(response_data)
            
            if len(response_data) < per_page:
                break
                
            page += 1
            
        return commits
    
    def get_commit_details(self, owner: str, repo: str, sha: str) -> Dict:
        """Get detailed commit information including file changes."""
        url = f"{self.base_url}/repos/{owner}/{repo}/commits/{sha}"
        return self._make_request(url)
    
    def get_file_content(self, owner: str, repo: str, sha: str, path: str) -> Optional[bytes]:
        """Get file content at a specific commit."""
        try:
            url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
            params = {'ref': sha}
            response_data = self._make_request(url, params)
            
            if response_data.get('encoding') == 'base64':
                import base64
                return base64.b64decode(response_data['content'])
            else:
                return response_data.get('content', '').encode('utf-8')
                
        except Exception as e:
            logger.warning(f"Failed to get file content for {path} at {sha}: {e}")
            return None


class CommitDataCollector:
    """Main class for collecting commit data from GitHub repositories."""
    
    def __init__(self, github_token: str, storage_backend=None):
        self.github_client = GitHubAPIClient(github_token)
        self.storage_backend = storage_backend
        
        # In-memory storage for collected data (for demonstration)
        self.repositories = {}
        self.authors = {}
        self.commits = {}
        self.file_changes = {}
    
    def _extract_file_type(self, file_path: str) -> str:
        """Extract file type from file path."""
        if '.' in file_path:
            return file_path.split('.')[-1].lower()
        return 'unknown'
    
    def _is_binary_file(self, content: Optional[bytes]) -> bool:
        """Simple heuristic to determine if file is binary."""
        if content is None:
            return False
        
        # Check for null bytes (common in binary files)
        return b'\x00' in content[:1024]  # Check first 1KB
    
    def _generate_s3_key(self, prefix: str, identifier: str, extension: str = '') -> str:
        """Generate S3 key for storing content."""
        return f"{prefix}/{identifier}{extension}"
    
    def collect_repository_data(self, owner: str, repo_name: str, 
                              max_commits: Optional[int] = None) -> Dict[str, Any]:
        """Collect all data for a repository."""
        logger.info(f"Starting data collection for {owner}/{repo_name}")
        
        try:
            # Get repository information
            repo_data = self.github_client.get_repository(owner, repo_name)
            repository = self._process_repository(repo_data)
            
            # Get commits
            commits_data = self.github_client.get_commits(owner, repo_name)
            
            if max_commits:
                commits_data = commits_data[:max_commits]
            
            logger.info(f"Processing {len(commits_data)} commits")
            
            processed_commits = []
            processed_file_changes = []
            
            for i, commit_data in enumerate(commits_data):
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{len(commits_data)} commits")
                
                # Get detailed commit information
                detailed_commit = self.github_client.get_commit_details(
                    owner, repo_name, commit_data['sha']
                )
                
                # Process commit
                commit = self._process_commit(detailed_commit, repository.repository_id)
                processed_commits.append(commit)
                
                # Process file changes
                file_changes = self._process_file_changes(
                    detailed_commit, owner, repo_name
                )
                processed_file_changes.extend(file_changes)
            
            logger.info(f"Completed data collection for {owner}/{repo_name}")
            
            return {
                'repository': repository,
                'commits': processed_commits,
                'file_changes': processed_file_changes,
                'authors': list(self.authors.values())
            }
            
        except Exception as e:
            logger.error(f"Failed to collect data for {owner}/{repo_name}: {e}")
            raise
    
    def _process_repository(self, repo_data: Dict) -> Repository:
        """Process repository data from GitHub API."""
        repository = Repository(
            repository_id=repo_data['html_url'],
            name=repo_data['name'],
            owner=repo_data['owner']['login'],
            description=repo_data.get('description', ''),
            primary_language=repo_data.get('language', ''),
            clone_url=repo_data['clone_url'],
            created_at=repo_data['created_at'],
            last_updated_at=repo_data['updated_at'],
            metadata={
                'stars': repo_data.get('stargazers_count', 0),
                'forks': repo_data.get('forks_count', 0),
                'size': repo_data.get('size', 0),
                'default_branch': repo_data.get('default_branch', 'main'),
                'topics': repo_data.get('topics', [])
            }
        )
        
        self.repositories[repository.repository_id] = repository
        return repository
    
    def _process_author(self, author_data: Dict) -> Author:
        """Process author data and store if not exists."""
        email = author_data.get('email', '')
        name = author_data.get('name', '')
        
        # Use email as primary identifier, fallback to name
        author_id = email if email else name
        
        if author_id not in self.authors:
            author = Author(
                author_id=author_id,
                name=name,
                username='',  # Not available in commit author data
                email=email
            )
            self.authors[author_id] = author
        
        return self.authors[author_id]
    
    def _process_commit(self, commit_data: Dict, repository_id: str) -> Commit:
        """Process commit data from GitHub API."""
        # Process author and committer
        author = self._process_author(commit_data['commit']['author'])
        committer = self._process_author(commit_data['commit']['committer'])
        
        # Extract statistics
        stats = commit_data.get('stats', {})
        
        commit = Commit(
            commit_hash=commit_data['sha'],
            repository_id=repository_id,
            author_id=author.author_id,
            committer_id=committer.author_id,
            message=commit_data['commit']['message'],
            authored_timestamp=commit_data['commit']['author']['date'],
            committed_timestamp=commit_data['commit']['committer']['date'],
            parent_hashes=[parent['sha'] for parent in commit_data.get('parents', [])],
            tree_hash=commit_data['commit']['tree']['sha'],
            stats_lines_added=stats.get('additions', 0),
            stats_lines_deleted=stats.get('deletions', 0),
            stats_files_changed=len(commit_data.get('files', []))
        )
        
        self.commits[commit.commit_hash] = commit
        return commit
    
    def _process_file_changes(self, commit_data: Dict, owner: str, repo_name: str) -> List[FileChange]:
        """Process file changes from commit data."""
        file_changes = []
        
        for file_data in commit_data.get('files', []):
            file_path = file_data['filename']
            file_change_id = f"{commit_data['sha']}_{file_path.replace('/', '_')}"
            
            # Determine change type
            status = file_data['status']
            change_type_map = {
                'added': 'ADDED',
                'modified': 'MODIFIED',
                'removed': 'DELETED',
                'renamed': 'RENAMED'
            }
            change_type = change_type_map.get(status, 'MODIFIED')
            
            # Get file content for before and after states
            content_before = None
            content_after = None
            
            if change_type != 'ADDED' and len(commit_data.get('parents', [])) > 0:
                parent_sha = commit_data['parents'][0]['sha']
                content_before = self.github_client.get_file_content(
                    owner, repo_name, parent_sha, file_path
                )
            
            if change_type != 'DELETED':
                content_after = self.github_client.get_file_content(
                    owner, repo_name, commit_data['sha'], file_path
                )
            
            # Generate blob hashes and S3 keys
            blob_hash_before = None
            blob_hash_after = None
            content_before_s3_key = None
            content_after_s3_key = None
            
            if content_before:
                blob_hash_before = hashlib.sha1(content_before).hexdigest()
                content_before_s3_key = self._generate_s3_key(
                    'file_blobs', blob_hash_before
                )
            
            if content_after:
                blob_hash_after = hashlib.sha1(content_after).hexdigest()
                content_after_s3_key = self._generate_s3_key(
                    'file_blobs', blob_hash_after
                )
            
            # Generate patch S3 key
            patch_s3_key = self._generate_s3_key(
                f'file_patches/{commit_data["sha"]}',
                file_path.replace('/', '_'),
                '.patch'
            )
            
            file_change = FileChange(
                file_change_id=file_change_id,
                commit_hash=commit_data['sha'],
                file_path=file_path,
                change_type=change_type,
                old_file_path=file_data.get('previous_filename'),
                lines_added=file_data.get('additions', 0),
                lines_deleted=file_data.get('deletions', 0),
                file_mode_before=None,  # Not available in GitHub API response
                file_mode_after=None,   # Not available in GitHub API response
                blob_hash_before=blob_hash_before,
                blob_hash_after=blob_hash_after,
                content_before_s3_key=content_before_s3_key,
                content_after_s3_key=content_after_s3_key,
                patch_s3_key=patch_s3_key,
                file_type=self._extract_file_type(file_path),
                is_binary=self._is_binary_file(content_after or content_before)
            )
            
            file_changes.append(file_change)
            self.file_changes[file_change_id] = file_change
        
        return file_changes
    
    def save_to_json(self, output_dir: str, data: Dict[str, Any]):
        """Save collected data to JSON files."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Save repository data
        with open(f"{output_dir}/repository.json", 'w') as f:
            json.dump(asdict(data['repository']), f, indent=2)
        
        # Save commits data
        with open(f"{output_dir}/commits.json", 'w') as f:
            json.dump([asdict(commit) for commit in data['commits']], f, indent=2)
        
        # Save file changes data
        with open(f"{output_dir}/file_changes.json", 'w') as f:
            json.dump([asdict(fc) for fc in data['file_changes']], f, indent=2)
        
        # Save authors data
        with open(f"{output_dir}/authors.json", 'w') as f:
            json.dump([asdict(author) for author in data['authors']], f, indent=2)
        
        logger.info(f"Data saved to {output_dir}")


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python collector.py <github_token> <owner> <repo> [max_commits]")
        sys.exit(1)
    
    github_token = sys.argv[1]
    owner = sys.argv[2]
    repo = sys.argv[3]
    max_commits = int(sys.argv[4]) if len(sys.argv) > 4 else None
    
    collector = CommitDataCollector(github_token)
    data = collector.collect_repository_data(owner, repo, max_commits)
    
    output_dir = f"output/{owner}_{repo}"
    collector.save_to_json(output_dir, data)

