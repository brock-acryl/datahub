"""Main MCP server implementation."""

import logging
from typing import Any

from mcp.server import Server
from mcp.types import Tool

from datahub_mcp_server.clients import GraphQLClient, OpenAPIClient
from datahub_mcp_server.config import Config
from datahub_mcp_server.models import EntityIdentifier
from datahub_mcp_server.platform import PlatformRegistry
from datahub_mcp_server.tools import DataHubTools
from datahub_mcp_server.utils import UrnResolver

logger = logging.getLogger(__name__)


class DataHubMCPServer:
    """DataHub MCP Server.

    Exposes DataHub operations as MCP tools for LLM interaction.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.server = Server("datahub-mcp-server")

        self.openapi_client = OpenAPIClient(config.datahub)
        self.graphql_client = GraphQLClient(config.datahub)
        self.urn_resolver = UrnResolver(config.datahub.default_env)
        self.platform_registry = PlatformRegistry()

        self.tools = DataHubTools(
            openapi_client=self.openapi_client,
            graphql_client=self.graphql_client,
            urn_resolver=self.urn_resolver,
        )

        self._register_tools()

    def _register_tools(self) -> None:
        """Register MCP tools."""

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """List available MCP tools."""
            return [
                Tool(
                    name="search_assets",
                    description=(
                        "Search for assets in DataHub. Supports filtering by entity type, "
                        "platform, environment, tags, domains, and more. Returns paginated "
                        "results with facets."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Search query string",
                            },
                            "types": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional entity types to filter (dataset, dashboard, etc.)",
                            },
                            "platform": {
                                "type": "string",
                                "description": "Optional platform filter (snowflake, bigquery, etc.)",
                            },
                            "env": {
                                "type": "string",
                                "description": "Optional environment filter (PROD, DEV, etc.)",
                            },
                            "filters": {
                                "type": "object",
                                "description": "Optional additional filters (tags, domains, etc.)",
                            },
                            "start": {
                                "type": "integer",
                                "description": "Result offset for pagination",
                                "default": 0,
                            },
                            "count": {
                                "type": "integer",
                                "description": "Number of results to return",
                                "default": 10,
                            },
                        },
                        "required": ["query"],
                    },
                ),
                Tool(
                    name="get_entity_full",
                    description=(
                        "Get full entity state including all aspects. Retrieves complete "
                        "metadata for a specific entity by URN or human-friendly identifier. "
                        "Includes schema, ownership, tags, glossary terms, and more."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {
                                "type": "string",
                                "description": "Entity URN (e.g., urn:li:dataset:...)",
                            },
                            "entity_type": {
                                "type": "string",
                                "description": "Entity type (dataset, dashboard, etc.)",
                            },
                            "platform": {
                                "type": "string",
                                "description": "Data platform (snowflake, bigquery, etc.)",
                            },
                            "platform_instance": {
                                "type": "string",
                                "description": "Platform instance identifier",
                            },
                            "name": {
                                "type": "string",
                                "description": "Dataset name (e.g., db.schema.table)",
                            },
                            "env": {
                                "type": "string",
                                "description": "Environment (PROD, DEV, etc.)",
                            },
                            "aspects": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional list of aspect names (omit for all)",
                            },
                        },
                        "oneOf": [
                            {"required": ["urn"]},
                            {"required": ["entity_type", "platform", "name"]},
                        ],
                    },
                ),
                Tool(
                    name="list_platforms",
                    description=(
                        "List all supported data platforms with their metadata conventions. "
                        "Shows platform naming patterns, supported entity types, and "
                        "platform instance support."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {},
                    },
                ),
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any]) -> list[dict[str, Any]]:
            """Handle tool calls."""
            try:
                if name == "search_assets":
                    return await self._handle_search_assets(arguments)
                elif name == "get_entity_full":
                    return await self._handle_get_entity_full(arguments)
                elif name == "list_platforms":
                    return await self._handle_list_platforms(arguments)
                else:
                    return [{"type": "text", "text": f"Unknown tool: {name}"}]
            except Exception as e:
                logger.exception(f"Error handling tool {name}")
                return [{"type": "text", "text": f"Error: {str(e)}"}]

    async def _handle_search_assets(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle search_assets tool call."""
        query = arguments.get("query", "")
        types = arguments.get("types")
        platform = arguments.get("platform")
        env = arguments.get("env")
        filters = arguments.get("filters")
        start = arguments.get("start", 0)
        count = arguments.get("count", 10)

        response = await self.tools.search_assets(
            query=query,
            types=types,
            platform=platform,
            env=env,
            filters=filters,
            start=start,
            count=count,
        )

        return [
            {
                "type": "text",
                "text": response.model_dump_json(indent=2),
            }
        ]

    async def _handle_get_entity_full(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle get_entity_full tool call."""
        identifier = EntityIdentifier(**arguments)
        aspects = arguments.get("aspects")

        response = await self.tools.get_entity_full(
            identifier=identifier,
            aspects=aspects,
        )

        return [
            {
                "type": "text",
                "text": response.model_dump_json(indent=2),
            }
        ]

    async def _handle_list_platforms(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle list_platforms tool call."""
        platforms = []

        for platform_name in self.platform_registry.list_platforms():
            info = self.platform_registry.get_platform(platform_name)
            if info:
                platforms.append(
                    {
                        "name": info.platform_name,
                        "entity_types": list(info.entity_types),
                        "dataset_naming_pattern": info.dataset_naming_pattern,
                        "supports_platform_instance": info.supports_platform_instance,
                        "default_aspects": info.default_aspects,
                    }
                )

        import json

        return [
            {
                "type": "text",
                "text": json.dumps({"platforms": platforms}, indent=2),
            }
        ]

    async def cleanup(self) -> None:
        """Cleanup server resources."""
        await self.openapi_client.close()
        await self.graphql_client.close()


__all__ = ["DataHubMCPServer"]
