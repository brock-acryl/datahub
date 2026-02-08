"""Data models for DataHub MCP Server."""

from typing import Any, Optional

from pydantic import BaseModel, Field


class EntityIdentifier(BaseModel):
    """Entity identifier supporting both URN and human-friendly formats."""

    urn: Optional[str] = Field(
        default=None,
        description="DataHub URN (e.g., urn:li:dataset:...)",
    )

    entity_type: Optional[str] = Field(
        default=None,
        description="Entity type (dataset, dashboard, chart, etc.)",
    )

    platform: Optional[str] = Field(
        default=None,
        description="Data platform (snowflake, bigquery, mysql, etc.)",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance identifier for multi-instance platforms",
    )

    name: Optional[str] = Field(
        default=None,
        description="Dataset name (e.g., db.schema.table)",
    )

    env: Optional[str] = Field(
        default="PROD",
        description="Environment/fabric (PROD, DEV, etc.)",
    )


class EntityAspect(BaseModel):
    """Entity aspect with value and system metadata."""

    name: str = Field(description="Aspect name")
    value: dict[str, Any] = Field(description="Aspect value as JSON")
    version: Optional[str] = Field(
        default=None,
        description="System metadata version for conditional writes",
    )
    last_modified: Optional[str] = Field(
        default=None,
        description="Last modification timestamp",
    )


class EntityFull(BaseModel):
    """Full entity representation with all aspects."""

    urn: str = Field(description="Entity URN")
    entity_type: str = Field(description="Entity type")
    aspects: list[EntityAspect] = Field(
        default_factory=list,
        description="All entity aspects",
    )


class SearchResult(BaseModel):
    """Search result item."""

    urn: str = Field(description="Entity URN")
    entity_type: str = Field(description="Entity type")
    name: Optional[str] = Field(default=None, description="Display name")
    description: Optional[str] = Field(default=None, description="Description")
    platform: Optional[str] = Field(default=None, description="Data platform")
    tags: list[str] = Field(default_factory=list, description="Tags")
    glossary_terms: list[str] = Field(default_factory=list, description="Glossary terms")
    owners: list[str] = Field(default_factory=list, description="Owners")
    domain: Optional[str] = Field(default=None, description="Domain")


class SearchResponse(BaseModel):
    """Search response with results and pagination."""

    results: list[SearchResult] = Field(default_factory=list, description="Search results")
    total: int = Field(description="Total number of results")
    page: int = Field(default=0, description="Current page (0-indexed)")
    page_size: int = Field(description="Page size")
    facets: dict[str, list[dict[str, Any]]] = Field(
        default_factory=dict,
        description="Search facets",
    )


class LineageNode(BaseModel):
    """Lineage graph node."""

    urn: str = Field(description="Entity URN")
    entity_type: str = Field(description="Entity type")
    name: Optional[str] = Field(default=None, description="Display name")


class LineageEdge(BaseModel):
    """Lineage graph edge."""

    source: str = Field(description="Source entity URN")
    destination: str = Field(description="Destination entity URN")
    relationship_type: str = Field(description="Relationship type")


class LineageResponse(BaseModel):
    """Lineage response."""

    nodes: list[LineageNode] = Field(default_factory=list, description="Lineage nodes")
    edges: list[LineageEdge] = Field(default_factory=list, description="Lineage edges")
    depth: int = Field(description="Lineage depth retrieved")


class PatchOperation(BaseModel):
    """JSON Patch operation with DataHub extensions."""

    op: str = Field(description="Operation type (add, remove, replace)")
    path: str = Field(description="JSON path to target field")
    value: Optional[Any] = Field(default=None, description="Value for add/replace operations")
    array_primary_keys: Optional[dict[str, str]] = Field(
        default=None,
        description="Array primary keys for idempotent array operations",
    )


class BulkOperationResult(BaseModel):
    """Result of a bulk operation."""

    success_count: int = Field(description="Number of successful operations")
    failure_count: int = Field(description="Number of failed operations")
    failures: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Details of failed operations",
    )
    operation_id: Optional[str] = Field(
        default=None,
        description="Operation ID for async tracking",
    )


__all__ = [
    "EntityIdentifier",
    "EntityAspect",
    "EntityFull",
    "SearchResult",
    "SearchResponse",
    "LineageNode",
    "LineageEdge",
    "LineageResponse",
    "PatchOperation",
    "BulkOperationResult",
]
