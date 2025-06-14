import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from minio import Minio
from minio.error import S3Error
import io
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3StorageBackend:
    """S3-like storage backend using MinIO."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 bucket_name: str, secure: bool = False):
        """Initialize S3 storage backend."""
        self.bucket_name = bucket_name
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        
        # Create bucket if it doesn't exist
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if not."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket already exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            raise
    
    def upload_object(self, object_key: str, data: bytes, content_type: str = 'application/octet-stream') -> str:
        """Upload object to S3."""
        try:
            data_stream = io.BytesIO(data)
            self.client.put_object(
                self.bucket_name,
                object_key,
                data_stream,
                length=len(data),
                content_type=content_type
            )
            logger.debug(f"Uploaded object: {object_key}")
            return f"s3://{self.bucket_name}/{object_key}"
        except S3Error as e:
            logger.error(f"Error uploading object {object_key}: {e}")
            raise
    
    def upload_file(self, object_key: str, file_path: str) -> str:
        """Upload file to S3."""
        try:
            self.client.fput_object(self.bucket_name, object_key, file_path)
            logger.debug(f"Uploaded file: {object_key}")
            return f"s3://{self.bucket_name}/{object_key}"
        except S3Error as e:
            logger.error(f"Error uploading file {object_key}: {e}")
            raise
    
    def download_object(self, object_key: str) -> bytes:
        """Download object from S3."""
        try:
            response = self.client.get_object(self.bucket_name, object_key)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Error downloading object {object_key}: {e}")
            raise
    
    def list_objects(self, prefix: str = "") -> List[str]:
        """List objects with given prefix."""
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects with prefix {prefix}: {e}")
            raise


class DataTransformer:
    """Transform collected JSON data to Parquet format with partitioning."""
    
    def __init__(self, storage_backend: S3StorageBackend):
        self.storage = storage_backend
    
    def transform_and_store_repository_data(self, repository_data: Dict[str, Any], 
                                          repository_id: str) -> Dict[str, str]:
        """Transform repository data and store as Parquet."""
        # Create DataFrame
        df = pd.DataFrame([repository_data])
        
        # Add partition columns
        df['repository_id_partition'] = repository_id.replace('/', '_').replace(':', '_')
        
        # Convert to Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_data = parquet_buffer.getvalue()
        
        # Generate object key with partitioning
        object_key = f"repositories_metadata/repository_id={df['repository_id_partition'].iloc[0]}/repository.parquet"
        
        # Upload to storage
        s3_key = self.storage.upload_object(object_key, parquet_data, 'application/octet-stream')
        
        logger.info(f"Stored repository metadata: {s3_key}")
        return {"repository_metadata_s3_key": s3_key}
    
    def transform_and_store_commits_data(self, commits_data: List[Dict[str, Any]], 
                                       repository_id: str) -> Dict[str, str]:
        """Transform commits data and store as Parquet with date partitioning."""
        if not commits_data:
            return {}
        
        # Create DataFrame
        df = pd.DataFrame(commits_data)
        
        # Add partition columns
        df['repository_id_partition'] = repository_id.replace('/', '_').replace(':', '_')
        
        # Parse timestamps for date partitioning
        df['authored_timestamp'] = pd.to_datetime(df['authored_timestamp'])
        df['year'] = df['authored_timestamp'].dt.year
        df['month'] = df['authored_timestamp'].dt.month
        
        # Group by year and month for partitioning
        stored_keys = []
        
        for (year, month), group_df in df.groupby(['year', 'month']):
            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            group_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_data = parquet_buffer.getvalue()
            
            # Generate object key with partitioning
            object_key = (f"commits_metadata/"
                         f"repository_id={group_df['repository_id_partition'].iloc[0]}/"
                         f"year={year}/month={month:02d}/commits.parquet")
            
            # Upload to storage
            s3_key = self.storage.upload_object(object_key, parquet_data, 'application/octet-stream')
            stored_keys.append(s3_key)
        
        logger.info(f"Stored commits metadata in {len(stored_keys)} partitions")
        return {"commits_metadata_s3_keys": stored_keys}
    
    def transform_and_store_file_changes_data(self, file_changes_data: List[Dict[str, Any]], 
                                            repository_id: str) -> Dict[str, str]:
        """Transform file changes data and store as Parquet with partitioning."""
        if not file_changes_data:
            return {}
        
        # Create DataFrame
        df = pd.DataFrame(file_changes_data)
        
        # Add partition columns
        df['repository_id_partition'] = repository_id.replace('/', '_').replace(':', '_')
        
        # Convert to Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_data = parquet_buffer.getvalue()
        
        # Generate object key with partitioning
        object_key = (f"file_changes_metadata/"
                     f"repository_id={df['repository_id_partition'].iloc[0]}/"
                     f"file_changes.parquet")
        
        # Upload to storage
        s3_key = self.storage.upload_object(object_key, parquet_data, 'application/octet-stream')
        
        logger.info(f"Stored file changes metadata: {s3_key}")
        return {"file_changes_metadata_s3_key": s3_key}
    
    def transform_and_store_authors_data(self, authors_data: List[Dict[str, Any]]) -> Dict[str, str]:
        """Transform authors data and store as Parquet."""
        if not authors_data:
            return {}
        
        # Create DataFrame
        df = pd.DataFrame(authors_data)
        
        # Convert to Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_data = parquet_buffer.getvalue()
        
        # Generate object key
        object_key = "authors_metadata/authors.parquet"
        
        # Upload to storage
        s3_key = self.storage.upload_object(object_key, parquet_data, 'application/octet-stream')
        
        logger.info(f"Stored authors metadata: {s3_key}")
        return {"authors_metadata_s3_key": s3_key}


class ContentStorage:
    """Handle storage of file contents and patches."""
    
    def __init__(self, storage_backend: S3StorageBackend):
        self.storage = storage_backend
    
    def store_file_content(self, content: bytes, blob_hash: str) -> str:
        """Store file content using blob hash as key."""
        object_key = f"file_blobs/{blob_hash}"
        return self.storage.upload_object(object_key, content, 'application/octet-stream')
    
    def store_patch(self, patch_content: str, commit_hash: str, file_path: str) -> str:
        """Store patch content."""
        # Escape file path for use in object key
        escaped_path = file_path.replace('/', '_').replace('\\', '_')
        object_key = f"file_patches/{commit_hash}/{escaped_path}.patch"
        
        patch_bytes = patch_content.encode('utf-8')
        return self.storage.upload_object(object_key, patch_bytes, 'text/plain')
    
    def get_file_content(self, blob_hash: str) -> bytes:
        """Retrieve file content by blob hash."""
        object_key = f"file_blobs/{blob_hash}"
        return self.storage.download_object(object_key)
    
    def get_patch(self, commit_hash: str, file_path: str) -> str:
        """Retrieve patch content."""
        escaped_path = file_path.replace('/', '_').replace('\\', '_')
        object_key = f"file_patches/{commit_hash}/{escaped_path}.patch"
        
        patch_bytes = self.storage.download_object(object_key)
        return patch_bytes.decode('utf-8')


class DataPipeline:
    """Complete data pipeline for processing and storing commit data."""
    
    def __init__(self, storage_config: Dict[str, Any]):
        """Initialize data pipeline with storage configuration."""
        self.storage = S3StorageBackend(**storage_config)
        self.transformer = DataTransformer(self.storage)
        self.content_storage = ContentStorage(self.storage)
    
    def process_collected_data(self, data_dir: str) -> Dict[str, Any]:
        """Process collected JSON data and store in S3-like storage."""
        logger.info(f"Processing data from {data_dir}")
        
        results = {}
        
        try:
            # Load JSON data
            with open(f"{data_dir}/repository.json", 'r') as f:
                repository_data = json.load(f)
            
            with open(f"{data_dir}/commits.json", 'r') as f:
                commits_data = json.load(f)
            
            with open(f"{data_dir}/file_changes.json", 'r') as f:
                file_changes_data = json.load(f)
            
            with open(f"{data_dir}/authors.json", 'r') as f:
                authors_data = json.load(f)
            
            repository_id = repository_data['repository_id']
            
            # Transform and store metadata
            results.update(self.transformer.transform_and_store_repository_data(
                repository_data, repository_id
            ))
            
            results.update(self.transformer.transform_and_store_commits_data(
                commits_data, repository_id
            ))
            
            results.update(self.transformer.transform_and_store_file_changes_data(
                file_changes_data, repository_id
            ))
            
            results.update(self.transformer.transform_and_store_authors_data(
                authors_data
            ))
            
            logger.info(f"Successfully processed data from {data_dir}")
            return results
            
        except Exception as e:
            logger.error(f"Error processing data from {data_dir}: {e}")
            raise
    
    def query_commits_by_repository(self, repository_id: str) -> pd.DataFrame:
        """Example query: Get all commits for a repository."""
        try:
            # List objects with the repository partition
            repo_partition = repository_id.replace('/', '_').replace(':', '_')
            prefix = f"commits_metadata/repository_id={repo_partition}/"
            
            object_keys = self.storage.list_objects(prefix)
            
            # Download and combine all Parquet files
            dataframes = []
            for key in object_keys:
                if key.endswith('.parquet'):
                    parquet_data = self.storage.download_object(key)
                    df = pd.read_parquet(io.BytesIO(parquet_data))
                    dataframes.append(df)
            
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                return combined_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error querying commits for repository {repository_id}: {e}")
            raise
    
    def query_file_changes_by_type(self, repository_id: str, file_type: str) -> pd.DataFrame:
        """Example query: Get file changes by file type."""
        try:
            # Get file changes data
            repo_partition = repository_id.replace('/', '_').replace(':', '_')
            object_key = f"file_changes_metadata/repository_id={repo_partition}/file_changes.parquet"
            
            parquet_data = self.storage.download_object(object_key)
            df = pd.read_parquet(io.BytesIO(parquet_data))
            
            # Filter by file type
            filtered_df = df[df['file_type'] == file_type]
            return filtered_df
            
        except Exception as e:
            logger.error(f"Error querying file changes by type {file_type}: {e}")
            raise


if __name__ == "__main__":
    # Example usage
    storage_config = {
        'endpoint': 'localhost:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin',
        'bucket_name': 'commit-data',
        'secure': False
    }
    
    pipeline = DataPipeline(storage_config)
    
    # Process sample data (if available)
    sample_data_dir = "../github-commit-collector/sample_data/octocat_Hello-World"
    if os.path.exists(sample_data_dir):
        results = pipeline.process_collected_data(sample_data_dir)
        print("Processing results:", results)

