
# Data Schema for GitHub Commit Data

This document outlines the data schema for storing GitHub commit data, designed to support the ML team's Next Edit Prediction model and accommodate future data requests.

## Schema Entities

The schema is composed of the following main entities.

### 1. Repository

Stores information about the GitHub repository from which the commit data is sourced.

| Field Name         | Data Type     | Description                                                                 | Example                                      |
| ------------------ | ------------- | --------------------------------------------------------------------------- | -------------------------------------------- |
| `repository_id`    | String        | Unique identifier for the repository (e.g., GitHub repository ID or URL). | `"https://github.com/owner/repo"`            |
| `name`             | String        | Name of the repository.                                                     | `"my-awesome-project"`                       |
| `owner`            | String        | Owner of the repository (user or organization).                             | `"john_doe"`                                 |
| `description`      | String        | Description of the repository.                                              | `"A project to demonstrate X and Y."`        |
| `primary_language` | String        | Primary programming language of the repository.                             | `"Python"`                                   |
| `clone_url`        | String        | URL to clone the repository.                                                | `"https://github.com/owner/repo.git"`        |
| `created_at`       | Timestamp     | Timestamp when the repository was created on GitHub.                        | `"2023-01-15T10:00:00Z"`                     |
| `last_updated_at`  | Timestamp     | Timestamp when the repository was last updated on GitHub.                   | `"2024-06-10T14:30:00Z"`                     |
| `metadata`         | JSON/Object   | Additional metadata (e.g., stars, forks, topics).                           | `{"stars": 100, "forks": 20}`              |

### 2. Author

Stores information about the commit author. This can be denormalized into the Commit entity or kept separate for normalization, depending on query patterns.

| Field Name      | Data Type | Description                                      | Example                          |
| --------------- | --------- | ------------------------------------------------ | -------------------------------- |
| `author_id`     | String    | Unique identifier for the author (e.g., email).  | `"john.doe@example.com"`         |
| `name`          | String    | Author's name.                                   | `"John Doe"`                     |
| `username`      | String    | Author's GitHub username (if available).         | `"john_doe_gh"`                  |
| `email`         | String    | Author's email address.                          | `"john.doe@example.com"`         |

### 3. Commit

Stores detailed information about each commit.

| Field Name          | Data Type     | Description                                                                    | Example                                      |
| ------------------- | ------------- | ------------------------------------------------------------------------------ | -------------------------------------------- |
| `commit_hash`       | String        | Unique SHA hash of the commit. (Primary Key for this entity)                   | `"a1b2c3d4e5f6..."`                          |
| `repository_id`     | String        | Foreign key referencing the Repository entity.                                 | `"https://github.com/owner/repo"`            |
| `author_id`         | String        | Foreign key referencing the Author entity (or denormalized author info).       | `"john.doe@example.com"`                     |
| `committer_id`      | String        | Foreign key referencing the Author entity for the committer (if different).    | `"jane.doe@example.com"`                     |
| `message`           | String        | Full commit message.                                                           | `"Fix: Corrected bug in user authentication."` |
| `authored_timestamp`| Timestamp     | Timestamp when the commit was authored.                                        | `"2024-06-14T10:30:00Z"`                     |
| `committed_timestamp`| Timestamp     | Timestamp when the commit was committed.                                       | `"2024-06-14T10:35:00Z"`                     |
| `parent_hashes`     | Array[String] | List of parent commit SHA hashes (for merge commits, this will have >1 entry). | `["f6e5d4c3b2a1..."]`                       |
| `tree_hash`         | String        | SHA hash of the Git tree object representing the repository state at this commit. | `"t1r2e3e4h5a6..."`                          |
| `stats_lines_added` | Integer       | Total lines added in this commit across all files.                             | `150`                                        |
| `stats_lines_deleted`| Integer       | Total lines deleted in this commit across all files.                           | `75`                                         |
| `stats_files_changed`| Integer       | Total number of files changed in this commit.                                  | `5`                                          |

### 4. FileChange

Stores information about each file modified in a commit, including its state before and after the commit. This is crucial for the Next Edit Prediction model.

| Field Name             | Data Type     | Description                                                                                                | Example                                         |
| ---------------------- | ------------- | ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `file_change_id`       | String        | Unique identifier for this file change instance (e.g., `commit_hash` + `file_path`).                         | `"a1b2c3d4e5f6_src/main.py"`                    |
| `commit_hash`          | String        | Foreign key referencing the Commit entity.                                                                 | `"a1b2c3d4e5f6..."`                             |
| `file_path`            | String        | Full path of the file within the repository.                                                               | `"src/main.py"`                                 |
| `change_type`          | Enum          | Type of change: `ADDED`, `MODIFIED`, `DELETED`, `RENAMED`, `COPIED`.                                       | `"MODIFIED"`                                    |
| `old_file_path`        | String        | Previous path of the file if renamed or copied (null otherwise).                                           | `"src/old_main.py"` (if renamed)                |
| `lines_added`          | Integer       | Number of lines added to this specific file.                                                               | `20`                                            |
| `lines_deleted`        | Integer       | Number of lines deleted from this specific file.                                                             | `10`                                            |
| `file_mode_before`     | String        | Git file mode before the commit (e.g., `100644` for regular file, `100755` for executable). Null if ADDED. | `"100644"`                                      |
| `file_mode_after`      | String        | Git file mode after the commit. Null if DELETED.                                                             | `"100644"`                                      |
| `blob_hash_before`     | String        | SHA hash of the Git blob object for the file content *before* the commit. Null if ADDED.                   | `"b1l2o3b4h5a6..."`                             |
| `blob_hash_after`      | String        | SHA hash of the Git blob object for the file content *after* the commit. Null if DELETED.                    | `"c1o2n3t4e5n6..."`                             |
| `content_before_s3_key`| String        | S3 key pointing to the raw file content *before* the commit. Null if ADDED. Stored separately for large files. | `"s3://bucket/content/b1l2o3b4h5a6.txt"`        |
| `content_after_s3_key` | String        | S3 key pointing to the raw file content *after* the commit. Null if DELETED. Stored separately for large files. | `"s3://bucket/content/c1o2n3t4e5n6.txt"`        |
| `patch_s3_key`         | String        | S3 key pointing to the diff/patch for this file change. Stored separately.                                 | `"s3://bucket/patches/a1b2c3d4e5f6_src_main.py.patch"` |
| `file_type`            | String        | Detected file type or extension (e.g., `py`, `java`, `md`). Used for filtering.                              | `"py"`                                          |
| `is_binary`            | Boolean       | Flag indicating if the file is binary.                                                                     | `false`                                         |

### 5. FileContent (Stored in S3, referenced by FileChange)

This is not a separate metadata entity but represents the actual file contents stored in the S3-like storage. The `FileChange` entity will contain S3 keys (`content_before_s3_key`, `content_after_s3_key`) pointing to these objects.

*   **Object Key Strategy**: `s3://<bucket-name>/file_contents/<blob_hash_or_unique_id>.<original_extension_if_known>`
*   **Content**: Raw byte stream of the file.

### 6. Patch (Stored in S3, referenced by FileChange)

Represents the diff/patch for a file change. Stored in the S3-like storage.

*   **Object Key Strategy**: `s3://<bucket-name>/patches/<commit_hash>/<escaped_file_path>.patch`
*   **Content**: Standard diff format (e.g., unified diff).

## Relationships

*   A `Repository` can have many `Commits`.
*   An `Author` can author many `Commits`.
*   A `Commit` is authored by one `Author` (and committed by one `Committer`, who can be the same or different).
*   A `Commit` involves changes to one or more files, represented by `FileChange` records.
*   Each `FileChange` record is associated with one `Commit`.
*   `FileChange` records reference `FileContent` and `Patch` objects stored in S3 via their S3 keys.