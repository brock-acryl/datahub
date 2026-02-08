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
                Tool(
                    name="add_tags",
                    description="Add tags to an entity. Tags must already exist in DataHub.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type (dataset, dashboard, etc.)"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment (PROD, DEV, etc.)"},
                            "tag_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of tag names to add",
                            },
                        },
                        "required": ["tag_names"],
                    },
                ),
                Tool(
                    name="remove_tags",
                    description="Remove tags from an entity.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "tag_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of tag names to remove",
                            },
                        },
                        "required": ["tag_names"],
                    },
                ),
                Tool(
                    name="add_glossary_terms",
                    description="Add glossary terms to an entity. Terms must already exist in DataHub.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "term_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of glossary term names to add",
                            },
                        },
                        "required": ["term_names"],
                    },
                ),
                Tool(
                    name="remove_glossary_terms",
                    description="Remove glossary terms from an entity.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "term_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of glossary term names to remove",
                            },
                        },
                        "required": ["term_names"],
                    },
                ),
                Tool(
                    name="add_owners",
                    description=(
                        "Add owners to an entity. "
                        "Owner type can be TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, or NONE."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "owner_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of owner usernames",
                            },
                            "owner_type": {
                                "type": "string",
                                "description": "Owner type",
                                "enum": ["TECHNICAL_OWNER", "BUSINESS_OWNER", "DATA_STEWARD", "NONE"],
                                "default": "NONE",
                            },
                        },
                        "required": ["owner_names"],
                    },
                ),
                Tool(
                    name="remove_owners",
                    description="Remove owners from an entity.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "owner_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of owner usernames to remove",
                            },
                        },
                        "required": ["owner_names"],
                    },
                ),
                Tool(
                    name="update_description",
                    description="Update the description of an entity.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "description": {
                                "type": "string",
                                "description": "New description text",
                            },
                        },
                        "required": ["description"],
                    },
                ),
                Tool(
                    name="set_domain",
                    description="Set the domain for an entity. Domain must already exist in DataHub.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "domain_name": {
                                "type": "string",
                                "description": "Domain name (not URN)",
                            },
                        },
                        "required": ["domain_name"],
                    },
                ),
                Tool(
                    name="unset_domain",
                    description="Remove the domain from an entity.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                        },
                    },
                ),
                Tool(
                    name="update_deprecation",
                    description="Update the deprecation status of an entity.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "urn": {"type": "string", "description": "Entity URN"},
                            "entity_type": {"type": "string", "description": "Entity type"},
                            "platform": {"type": "string", "description": "Data platform"},
                            "name": {"type": "string", "description": "Dataset name"},
                            "env": {"type": "string", "description": "Environment"},
                            "deprecated": {
                                "type": "boolean",
                                "description": "Whether the entity is deprecated",
                            },
                            "note": {
                                "type": "string",
                                "description": "Optional deprecation note",
                            },
                            "decommission_time": {
                                "type": "integer",
                                "description": "Optional decommission timestamp (milliseconds)",
                            },
                        },
                        "required": ["deprecated"],
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
                elif name == "add_tags":
                    return await self._handle_add_tags(arguments)
                elif name == "remove_tags":
                    return await self._handle_remove_tags(arguments)
                elif name == "add_glossary_terms":
                    return await self._handle_add_terms(arguments)
                elif name == "remove_glossary_terms":
                    return await self._handle_remove_terms(arguments)
                elif name == "add_owners":
                    return await self._handle_add_owners(arguments)
                elif name == "remove_owners":
                    return await self._handle_remove_owners(arguments)
                elif name == "update_description":
                    return await self._handle_update_description(arguments)
                elif name == "set_domain":
                    return await self._handle_set_domain(arguments)
                elif name == "unset_domain":
                    return await self._handle_unset_domain(arguments)
                elif name == "update_deprecation":
                    return await self._handle_update_deprecation(arguments)
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

    async def _handle_add_tags(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle add_tags tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "tag_names"})
        tag_names = arguments["tag_names"]

        await self.tools.add_tags(identifier, tag_names)

        return [{"type": "text", "text": f"Successfully added {len(tag_names)} tag(s)"}]

    async def _handle_remove_tags(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle remove_tags tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "tag_names"})
        tag_names = arguments["tag_names"]

        await self.tools.remove_tags(identifier, tag_names)

        return [{"type": "text", "text": f"Successfully removed {len(tag_names)} tag(s)"}]

    async def _handle_add_terms(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle add_glossary_terms tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "term_names"})
        term_names = arguments["term_names"]

        await self.tools.add_terms(identifier, term_names)

        return [{"type": "text", "text": f"Successfully added {len(term_names)} glossary term(s)"}]

    async def _handle_remove_terms(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle remove_glossary_terms tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "term_names"})
        term_names = arguments["term_names"]

        await self.tools.remove_terms(identifier, term_names)

        return [{"type": "text", "text": f"Successfully removed {len(term_names)} glossary term(s)"}]

    async def _handle_add_owners(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle add_owners tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k not in ["owner_names", "owner_type"]})
        owner_names = arguments["owner_names"]
        owner_type = arguments.get("owner_type", "NONE")

        await self.tools.add_owners(identifier, owner_names, owner_type)

        return [{"type": "text", "text": f"Successfully added {len(owner_names)} owner(s)"}]

    async def _handle_remove_owners(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle remove_owners tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "owner_names"})
        owner_names = arguments["owner_names"]

        await self.tools.remove_owners(identifier, owner_names)

        return [{"type": "text", "text": f"Successfully removed {len(owner_names)} owner(s)"}]

    async def _handle_update_description(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle update_description tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "description"})
        description = arguments["description"]

        await self.tools.update_description(identifier, description)

        return [{"type": "text", "text": "Successfully updated description"}]

    async def _handle_set_domain(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle set_domain tool call."""
        identifier = EntityIdentifier(**{k: v for k, v in arguments.items() if k != "domain_name"})
        domain_name = arguments["domain_name"]

        await self.tools.set_domain(identifier, domain_name)

        return [{"type": "text", "text": f"Successfully set domain to {domain_name}"}]

    async def _handle_unset_domain(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle unset_domain tool call."""
        identifier = EntityIdentifier(**arguments)

        await self.tools.unset_domain(identifier)

        return [{"type": "text", "text": "Successfully unset domain"}]

    async def _handle_update_deprecation(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """Handle update_deprecation tool call."""
        identifier = EntityIdentifier(
            **{k: v for k, v in arguments.items() if k not in ["deprecated", "note", "decommission_time"]}
        )
        deprecated = arguments["deprecated"]
        note = arguments.get("note")
        decommission_time = arguments.get("decommission_time")

        await self.tools.update_deprecation(identifier, deprecated, note, decommission_time)

        status = "deprecated" if deprecated else "not deprecated"
        return [{"type": "text", "text": f"Successfully updated deprecation status to {status}"}]

    async def cleanup(self) -> None:
        """Cleanup server resources."""
        await self.openapi_client.close()
        await self.graphql_client.close()


__all__ = ["DataHubMCPServer"]
