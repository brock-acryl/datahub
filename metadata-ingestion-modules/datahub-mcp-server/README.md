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

### Phase 1: Read-Only Operations (Current)

- Search assets with filters and facets
- Get full entity state (all aspects)
- Navigate lineage and relationships
- Platform-aware URN resolution

### Phase 2: Single-Entity Edits (Planned)

- Add/remove tags, terms, owners
- Update descriptions and documentation
- Set domains and deprecation status
- Patch-based updates for safety

### Phase 3: Bulk Operations (Planned)

- Bulk tag and term management
- Bulk ownership updates
- Conditional writes with version checking
- Async operation tracking

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
