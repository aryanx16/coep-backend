# Iceberg & Delta Lake API Documentation

## Overview
This API provides detailed metadata, statistics, version history, and data samples for Apache Iceberg and Delta Lake tables stored in S3. It also includes schema comparison functionality for Iceberg tables.

---

## 1. Get Iceberg Table Details

**Endpoint:** `GET /Iceberg`

**Description:** Retrieves metadata, statistics, version history, and a data sample for an Apache Iceberg table.

### Query Parameters:
- `s3_url` (string, required): The S3 path to the root directory of the Iceberg table (e.g., `s3://your-bucket/path/to/iceberg-table/`). Must end with `/`.

### Success Response (200 OK):
#### **Response Body:**
```json
{
  "table_type": "Iceberg",
  "table_uuid": "<unique-id>",
  "location": "<provided S3 URL>",
  "format_configuration": { ... },
  "iceberg_manifest_files": [ ... ],
  "format_version": 2,
  "current_snapshot_id": "<snapshot-id>",
  "table_schema": { ... },
  "partition_spec": { ... },
  "sort_order": { ... },
  "version_history": {
    "total_snapshots": 10,
    "current_snapshot_summary": { ... },
    "snapshots_overview": [ ... ]
  },
  "key_metrics": { ... },
  "partition_explorer": [ ... ],
  "sample_data": [ ... ]
}
```

### Error Responses:
- **400 Bad Request:** Missing or invalid `s3_url` parameter.
- **401 Unauthorized:** AWS credentials missing or invalid.
- **404 Not Found:** S3 bucket or metadata files not found.
- **500 Internal Server Error:** AWS client error or unexpected server errors.

---

## 2. Get Delta Lake Table Details

**Endpoint:** `GET /Delta`

**Description:** Retrieves metadata, statistics, version history, and a data sample for a Delta Lake table by reading its transaction log.

### Query Parameters:
- `s3_url` (string, required): The S3 path to the root directory of the Delta Lake table (e.g., `s3://your-bucket/path/to/delta-table/`). Must end with `/`.

### Success Response (200 OK):
#### **Response Body:**
```json
{
  "table_type": "Delta",
  "table_uuid": "<unique-id>",
  "location": "<provided S3 URL>",
  "format_configuration": { ... },
  "format_version": 2,
  "delta_log_files": [ ... ],
  "current_snapshot_id": "<commit-number>",
  "current_snapshot_details": { ... },
  "table_schema": { ... },
  "table_properties": { ... },
  "partition_spec": { ... },
  "sort_order": [],
  "version_history": {
    "total_snapshots": 10,
    "current_snapshot_summary": { ... },
    "snapshots_overview": [ ... ]
  },
  "key_metrics": { ... },
  "partition_explorer": [ ... ],
  "sample_data": [ ... ]
}
```

### Error Responses:
- **400 Bad Request:** Missing or invalid `s3_url` parameter.
- **401 Unauthorized:** AWS credentials missing or invalid.
- **404 Not Found:** S3 bucket or `_delta_log` directory missing.
- **500 Internal Server Error:** AWS client error or unexpected server errors.

---

## 3. Compare Iceberg Schemas

**Endpoint:** `GET /compare_schema/Iceberg`

**Description:** Compares the schemas of an Iceberg table between two specified sequence numbers.

### Query Parameters:
- `s3_url` (string, required): The S3 path to the root directory of the Iceberg table.
- `sequence_number_1` (integer, required): The first sequence number.
- `sequence_number_2` (integer, required): The second sequence number.

### Success Response (200 OK):
#### **Response Body:**
```json
{
  "table_type": "Iceberg",
  "schema_comparison": {
    "changes": [
      {
        "field": "column_name",
        "change_type": "added/removed/modified",
        "details": "<change details>"
      }
    ]
  }
}
```

### Error Responses:
- **400 Bad Request:** Missing or invalid parameters.
- **401 Unauthorized:** AWS credentials missing or invalid.
- **404 Not Found:** Table metadata missing.
- **500 Internal Server Error:** AWS client error or unexpected server errors.

---

## Authentication
AWS credentials are required for accessing S3 and must be provided via environment variables, IAM roles, or a configuration file.

---

## Usage Example
### Fetching Iceberg Table Metadata
```sh
curl -X GET "http://your-api-url/Iceberg?s3_url=s3://your-bucket/path/to/iceberg-table/"
```

### Fetching Delta Table Metadata
```sh
curl -X GET "http://your-api-url/Delta?s3_url=s3://your-bucket/path/to/delta-table/"
```

### Comparing Iceberg Table Schemas
```sh
curl -X GET "http://your-api-url/compare_schema/Iceberg?s3_url=s3://your-bucket/path/to/iceberg-table/&sequence_number_1=1&sequence_number_2=2"
```

---

## License
MIT License
