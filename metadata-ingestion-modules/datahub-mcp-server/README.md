# DataHub MCP Server

Model Context Protocol (MCP) server for DataHub, enabling LLM-powered interactions with DataHub's metadata platform.

## Overview

The DataHub MCP Server provides a standardized interface for AI assistants to interact with DataHub as a "power user" would through the UI. It supports:

- **Entity Discovery**: Search and navigate assets across the metadata catalog
- **Entity Reading**: Retrieve full entity state including all aspects
- **Metadata Editing**: Update ownership, documentation, tags, glossary terms, domains, and deprecation status
- **Bulk Operations**: Perform batch updates safely with conflict detection
- **Platform-Aware Operations**: Handle platform-specific URN conventions and metadata patterns

## Features

### Phase 1: Read-Only Operations ✅

- Search assets with filters and facets
- Get full entity state (all aspects)
- List supported platforms
- Platform-aware URN resolution

### Phase 2: Single-Entity Edits ✅

- **Tags**: Add/remove tags to/from entities
- **Glossary Terms**: Add/remove glossary terms to/from entities
- **Ownership**: Add/remove owners with owner types (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD)
- **Descriptions**: Update entity descriptions
- **Domains**: Set/unset domains for entities
- **Deprecation**: Update deprecation status with optional notes and decommission time
- GraphQL-based mutations for UI-parity operations

### Phase 3: Bulk Operations ✅

- **Bulk Tags**: Add/remove tags across multiple entities
- **Bulk Glossary Terms**: Add/remove glossary terms across multiple entities
- **Bulk Ownership**: Add/remove owners across multiple entities with owner types
- **Bulk Domains**: Set domains for multiple entities
- **Bulk Descriptions**: Update descriptions for multiple entities
- Detailed success/failure reporting with error details
- High-throughput batch processing

### Phase 4: Entity Creation ✅

- **Dataset Creation**: Create new datasets with platform-specific naming conventions
- **Container Creation**: Create containers (databases, schemas, projects)
- **Schema Definition**: Define dataset schemas with field types and metadata
- **Aspect-Complete**: Creates all required aspects (status, properties, schema, subTypes, etc.)
- **Golden File Patterns**: Follows DataHub connector conventions from golden test files
- **Platform-Aware**: Supports Snowflake, BigQuery, MySQL, Postgres, Redshift naming patterns

## Installation

```bash
# From the metadata-ingestion-modules/datahub-mcp-server directory
pip install -e .

# With development dependencies
pip install -e .[dev]
```

## Usage

```bash
# Start the MCP server
datahub-mcp-server --datahub-url http://localhost:8080 --token <your-token>
```

### Available MCP Tools

#### Discovery & Read Operations

- `search_assets` - Search for assets with filters (platform, env, tags, domains)
- `get_entity_full` - Get complete entity state with all aspects
- `list_platforms` - List supported platforms and their conventions

#### Tag Management

- `add_tags` - Add tags to an entity (tags must exist in DataHub)
- `remove_tags` - Remove tags from an entity

#### Glossary Term Management

- `add_glossary_terms` - Add glossary terms to an entity (terms must exist)
- `remove_glossary_terms` - Remove glossary terms from an entity

#### Ownership Management

- `add_owners` - Add owners with specified type (TECHNICAL_OWNER, BUSINESS_OWNER, etc.)
- `remove_owners` - Remove owners from an entity

#### Documentation & Metadata

- `update_description` - Update entity description
- `set_domain` - Set the domain for an entity (domain must exist)
- `unset_domain` - Remove domain from an entity
- `update_deprecation` - Mark entity as deprecated/not deprecated with optional note

#### Bulk Operations (Phase 3)

- `bulk_add_tags` - Add tags to multiple entities
- `bulk_remove_tags` - Remove tags from multiple entities
- `bulk_add_glossary_terms` - Add glossary terms to multiple entities
- `bulk_remove_glossary_terms` - Remove glossary terms from multiple entities
- `bulk_add_owners` - Add owners to multiple entities
- `bulk_remove_owners` - Remove owners from multiple entities
- `bulk_set_domain` - Set domain for multiple entities
- `bulk_update_descriptions` - Update descriptions for multiple entities

All bulk operations return detailed results with success/failure counts and error details.

#### Entity Creation (Phase 4)

- `create_dataset` - Create a new dataset entity with schema definition
  - Supports platform-specific naming: Snowflake (`db.schema.table`), BigQuery (`project.dataset.table`), MySQL (`database.table`)
  - Define schema fields with types (string, number, boolean, date, timestamp, bytes, array, record)
  - Set descriptions, subtypes (Table, View, External), container hierarchy
  - Creates all required aspects following DataHub conventions

- `create_container` - Create a new container entity (Database, Schema, Project, Dataset)
  - Organize entities in hierarchies
  - Set parent containers for nested structures
  - Platform-specific properties and metadata

**Example: Create a Snowflake dataset**
```json
{
  "platform": "snowflake",
  "name": "analytics_db.sales.customers",
  "schema_fields": [
    {
      "field_path": "customer_id",
      "native_data_type": "NUMBER(38,0)",
      "field_type": "number",
      "description": "Unique customer identifier",
      "is_part_of_key": true
    },
    {
      "field_path": "email",
      "native_data_type": "VARCHAR(255)",
      "field_type": "string",
      "description": "Customer email address"
    }
  ],
  "description": "Customer master data table",
  "subtype": "Table",
  "env": "PROD"
}
```

## Architecture

The server implements three complementary API "lanes":

1. **OpenAPI**: Bulk-safe reads/writes, conditional updates, generic patching
2. **GraphQL**: UI-parity operations for interactive edits
3. **Rest.li**: Direct MCP/MCL ingestion for advanced workflows

Platform-specific behavior is driven by a registry derived from DataHub connector golden test files, ensuring consistency with upstream ingestion patterns.

## Development

```bash
# Setup development environment
./gradlew :metadata-ingestion-modules:datahub-mcp-server:installDev

# Run linting
./gradlew :metadata-ingestion-modules:datahub-mcp-server:lint

# Fix linting issues
./gradlew :metadata-ingestion-modules:datahub-mcp-server:lintFix

# Run tests
./gradlew :metadata-ingestion-modules:datahub-mcp-server:testQuick
```

## License

Apache License 2.0
