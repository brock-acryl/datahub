# Phase 4: Platform-Specific Entity Creation

## Overview

Phase 4 adds the ability to CREATE entities and aspects in DataHub using platform-specific patterns derived from golden test files. This enables AI assistants to bootstrap new metadata entities that follow DataHub's ingestion connector conventions.

## Analysis: Golden Test File Patterns

### Dataset Creation Pattern (from golden files)

A complete dataset entity requires multiple aspects to be created:

#### Core Aspects (Required)
1. **status** - Entity lifecycle status
   ```json
   {
     "removed": false
   }
   ```

2. **datasetProperties** - Basic dataset metadata
   ```json
   {
     "name": "table-1",
     "qualifiedName": "project-id-1.bigquery-dataset-1.table-1",
     "description": "Dataset description",
     "customProperties": {},
     "externalUrl": "https://...",
     "tags": []
   }
   ```

3. **schemaMetadata** - Dataset schema with fields
   ```json
   {
     "schemaName": "project-id-1.bigquery-dataset-1.table-1",
     "platform": "urn:li:dataPlatform:bigquery",
     "version": 0,
     "fields": [
       {
         "fieldPath": "column_name",
         "nullable": false,
         "description": "Column description",
         "type": { "type": { "com.linkedin.schema.NumberType": {} } },
         "nativeDataType": "INT",
         "recursive": false,
         "isPartOfKey": false
       }
     ]
   }
   ```

4. **subTypes** - Entity subtype classification
   ```json
   {
     "typeNames": ["Table"]  // or "View", "External", etc.
   }
   ```

5. **container** - Parent container reference
   ```json
   {
     "container": "urn:li:container:<hash>"
   }
   ```

6. **dataPlatformInstance** - Platform linkage
   ```json
   {
     "platform": "urn:li:dataPlatform:bigquery"
   }
   ```

7. **browsePathsV2** - Navigation hierarchy
   ```json
   {
     "path": [
       {"id": "urn:li:container:<parent>", "urn": "urn:li:container:<parent>"}
     ]
   }
   ```

#### Optional Aspects
- **globalTags** - Tags applied to the dataset
- **upstreamLineage** - Lineage relationships
- **datasetProfile** - Profiling statistics
- **datasetUsageStatistics** - Usage metrics
- **operation** - Operations performed

### Container Creation Pattern

Containers (databases, schemas, projects) require:

1. **containerProperties** - Basic container info
   ```json
   {
     "name": "database_name",
     "customProperties": {
       "platform": "snowflake",
       "env": "PROD",
       "database": "database_name"
     },
     "externalUrl": "https://...",
     "env": "PROD"
   }
   ```

2. **status**, **dataPlatformInstance**, **subTypes**, **browsePathsV2**

### Platform-Specific URN Patterns

**Snowflake Dataset:**
```
urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_1,PROD)
```

**BigQuery Dataset:**
```
urn:li:dataset:(urn:li:dataPlatform:bigquery,project-id-1.dataset-1.table-1,PROD)
```

**MySQL Dataset:**
```
urn:li:dataset:(urn:li:dataPlatform:mysql,database.table,PROD)
```

**Container URN (hashed):**
```
urn:li:container:<md5_hash_of_key>
```

## Design: Entity Creation Architecture

### 1. Data Models

**SchemaField Model:**
```python
class SchemaField(BaseModel):
    field_path: str
    nullable: bool = False
    description: str = ""
    native_data_type: str
    field_type: str  # "number", "string", "boolean", "date", "timestamp", etc.
    is_part_of_key: bool = False
```

**DatasetCreateRequest Model:**
```python
class DatasetCreateRequest(BaseModel):
    platform: str
    name: str
    env: str = "PROD"
    platform_instance: str | None = None
    description: str = ""
    schema_fields: list[SchemaField] = []
    container_urn: str | None = None
    subtype: str = "Table"  # "Table", "View", "External"
    external_url: str | None = None
    custom_properties: dict[str, str] = {}
```

**ContainerCreateRequest Model:**
```python
class ContainerCreateRequest(BaseModel):
    platform: str
    name: str
    env: str = "PROD"
    container_type: str  # "Database", "Schema", "Project", "Dataset"
    description: str = ""
    parent_container_urn: str | None = None
    external_url: str | None = None
    custom_properties: dict[str, str] = {}
```

### 2. EntityCreator Class

```python
class EntityCreator:
    def __init__(self, openapi_client: OpenAPIClient, urn_resolver: UrnResolver):
        self.client = openapi_client
        self.urn_resolver = urn_resolver

    async def create_dataset(self, request: DatasetCreateRequest) -> str:
        """Create a dataset entity with all required aspects."""
        # 1. Generate URN
        # 2. Create status aspect
        # 3. Create datasetProperties aspect
        # 4. Create schemaMetadata aspect
        # 5. Create subTypes aspect
        # 6. Create container aspect (if container_urn provided)
        # 7. Create dataPlatformInstance aspect
        # 8. Create browsePathsV2 aspect
        # Return: dataset URN

    async def create_container(self, request: ContainerCreateRequest) -> str:
        """Create a container entity with all required aspects."""
        # 1. Generate URN (hashed)
        # 2. Create containerProperties aspect
        # 3. Create status aspect
        # 4. Create dataPlatformInstance aspect
        # 5. Create subTypes aspect
        # 6. Create browsePathsV2 aspect
        # 7. Create container aspect (if parent_container_urn provided)
        # Return: container URN
```

### 3. Aspect Builders

Create helper functions to build aspect JSON from models:

```python
def build_dataset_properties(request: DatasetCreateRequest) -> dict:
    """Build datasetProperties aspect JSON."""

def build_schema_metadata(request: DatasetCreateRequest) -> dict:
    """Build schemaMetadata aspect JSON."""

def build_container_properties(request: ContainerCreateRequest) -> dict:
    """Build containerProperties aspect JSON."""
```

### 4. Type Mapping

Map simple types to DataHub schema types:

```python
TYPE_MAPPING = {
    "string": {"type": {"com.linkedin.schema.StringType": {}}},
    "number": {"type": {"com.linkedin.schema.NumberType": {}}},
    "boolean": {"type": {"com.linkedin.schema.BooleanType": {}}},
    "date": {"type": {"com.linkedin.schema.DateType": {}}},
    "timestamp": {"type": {"com.linkedin.schema.TimeType": {}}},
    # ... more types
}
```

## Implementation Plan

### Step 1: Add Data Models
- Create `SchemaField`, `DatasetCreateRequest`, `ContainerCreateRequest` models
- Add type mapping dictionary
- Update models/__init__.py exports

### Step 2: Implement Aspect Builders
- `build_status_aspect()`
- `build_dataset_properties_aspect()`
- `build_schema_metadata_aspect()`
- `build_container_properties_aspect()`
- `build_subtypes_aspect()`
- `build_data_platform_instance_aspect()`
- `build_browse_paths_v2_aspect()`
- `build_container_aspect()`

### Step 3: Implement EntityCreator
- Create `EntityCreator` class in `src/datahub_mcp_server/entity_creator.py`
- Implement `create_dataset()` method
- Implement `create_container()` method
- Use OpenAPI client's `create_aspect()` or batch methods

### Step 4: Add MCP Tools
- `create_dataset` - Create a dataset with schema
- `create_container` - Create a container (database/schema)

### Step 5: Testing
- Test dataset creation for each platform (snowflake, bigquery, mysql, etc.)
- Test container creation
- Verify aspect structure matches golden files
- Test error handling

### Step 6: Documentation
- Update README with entity creation tools
- Add examples for each platform
- Document schema field type mappings

## API Design

### MCP Tool: create_dataset

```python
@mcp.tool()
async def create_dataset(
    platform: str,
    name: str,
    schema_fields: list[dict],  # [{field_path, native_data_type, field_type, description, nullable}]
    env: str = "PROD",
    platform_instance: str | None = None,
    description: str = "",
    container_urn: str | None = None,
    subtype: str = "Table",
) -> dict:
    """
    Create a new dataset entity in DataHub.

    Args:
        platform: Platform name (snowflake, bigquery, mysql, etc.)
        name: Dataset name (e.g., "db.schema.table" for Snowflake)
        schema_fields: List of field definitions
        env: Environment (PROD, DEV, etc.)
        platform_instance: Platform instance identifier
        description: Dataset description
        container_urn: Parent container URN (optional)
        subtype: Dataset subtype (Table, View, External)

    Returns:
        {"urn": "urn:li:dataset:...", "status": "created"}
    """
```

### MCP Tool: create_container

```python
@mcp.tool()
async def create_container(
    platform: str,
    name: str,
    container_type: str,  # "Database", "Schema", "Project", "Dataset"
    env: str = "PROD",
    description: str = "",
    parent_container_urn: str | None = None,
) -> dict:
    """
    Create a new container entity in DataHub.

    Args:
        platform: Platform name
        name: Container name
        container_type: Type of container (Database, Schema, etc.)
        env: Environment
        description: Container description
        parent_container_urn: Parent container URN for nested containers

    Returns:
        {"urn": "urn:li:container:...", "status": "created"}
    """
```

## Platform-Specific Considerations

### Snowflake
- Dataset naming: `database.schema.table`
- Container hierarchy: Database > Schema > Table
- Supports platform instances

### BigQuery
- Dataset naming: `project.dataset.table`
- Container hierarchy: Project > Dataset > Table
- Platform instance rarely used

### MySQL/Postgres
- Dataset naming: `database.table`
- Container hierarchy: Database > Table
- No schema level in URN

### Redshift
- Dataset naming: `database.schema.table`
- Similar to Snowflake pattern

## Error Handling

- Validate platform is supported
- Validate naming conventions per platform
- Check if entity already exists (optional conflict detection)
- Handle aspect creation failures gracefully
- Return detailed error messages

## Future Enhancements

- Schema inference from example data
- Automatic container creation if not exists
- Support for additional entity types (charts, dashboards, etc.)
- Lineage creation during dataset creation
- Column-level lineage support

## Success Criteria

1. Can create datasets for all supported platforms
2. Can create container hierarchies
3. Generated entities match golden file structure
4. All core aspects are properly created
5. Tests validate entity structure
6. Documentation is complete with examples
