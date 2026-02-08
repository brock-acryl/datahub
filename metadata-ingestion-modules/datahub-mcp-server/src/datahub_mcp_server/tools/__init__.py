"""MCP tools for DataHub operations."""

from typing import Any, Optional

from datahub_mcp_server.clients import GraphQLClient, OpenAPIClient
from datahub_mcp_server.models import (
    EntityFull,
    EntityIdentifier,
    LineageResponse,
    SearchResponse,
)
from datahub_mcp_server.utils import UrnResolver


class DataHubTools:
    """DataHub MCP tools implementation.

    Provides read and write operations on DataHub metadata,
    routing to appropriate API clients based on operation type.
    """

    def __init__(
        self,
        openapi_client: OpenAPIClient,
        graphql_client: GraphQLClient,
        urn_resolver: UrnResolver,
    ) -> None:
        self.openapi_client = openapi_client
        self.graphql_client = graphql_client
        self.urn_resolver = urn_resolver

    async def search_assets(
        self,
        query: str,
        types: Optional[list[str]] = None,
        platform: Optional[str] = None,
        env: Optional[str] = None,
        filters: Optional[dict[str, list[str]]] = None,
        start: int = 0,
        count: int = 10,
    ) -> SearchResponse:
        """Search for assets in DataHub.

        Args:
            query: Search query string
            types: Optional list of entity types to filter
            platform: Optional platform filter
            env: Optional environment filter
            filters: Optional additional filters (tags, domains, etc.)
            start: Result offset
            count: Number of results to return

        Returns:
            SearchResponse with results and facets
        """
        graphql_filters = []

        if platform:
            graphql_filters.append(
                {
                    "field": "platform",
                    "values": [f"urn:li:dataPlatform:{platform}"],
                }
            )

        if env:
            graphql_filters.append(
                {
                    "field": "origin",
                    "values": [env],
                }
            )

        if filters:
            for field, values in filters.items():
                graphql_filters.append(
                    {
                        "field": field,
                        "values": values,
                    }
                )

        response = await self.graphql_client.search(
            query=query,
            types=types,
            filters=graphql_filters if graphql_filters else None,
            start=start,
            count=count,
        )

        return self._parse_search_response(response)

    async def get_entity_full(
        self,
        identifier: EntityIdentifier,
        aspects: Optional[list[str]] = None,
    ) -> EntityFull:
        """Get full entity state with all aspects.

        Args:
            identifier: Entity identifier (URN or human-friendly format)
            aspects: Optional list of aspect names (None = all aspects)

        Returns:
            EntityFull with all entity aspects
        """
        urn = self.urn_resolver.resolve(identifier)
        entity_type = identifier.entity_type or "dataset"

        response = await self.openapi_client.batch_get_entities(
            entity_type=entity_type,
            urns=[urn],
            aspects=aspects,
            include_system_metadata=True,
        )

        return self._parse_entity_response(urn, entity_type, response)

    async def get_lineage(
        self,
        identifier: EntityIdentifier,
        direction: str = "both",
        depth: int = 1,
    ) -> LineageResponse:
        """Get entity lineage.

        Args:
            identifier: Entity identifier
            direction: Lineage direction (upstream, downstream, both)
            depth: Lineage depth to retrieve

        Returns:
            LineageResponse with nodes and edges

        Raises:
            NotImplementedError: Lineage retrieval not yet implemented
        """
        raise NotImplementedError("Lineage retrieval coming in next phase")

    def _parse_search_response(self, response: dict[str, Any]) -> SearchResponse:
        """Parse GraphQL search response into SearchResponse model."""
        from datahub_mcp_server.models import SearchResult

        search_data = response.get("search", {})
        results = []

        for item in search_data.get("searchResults", []):
            entity = item.get("entity", {})
            urn = entity.get("urn", "")
            entity_type = entity.get("type", "")

            tags = []
            if "tags" in entity and entity["tags"]:
                tags = [
                    tag["tag"]["name"]
                    for tag in entity["tags"].get("tags", [])
                    if "tag" in tag and "name" in tag["tag"]
                ]

            glossary_terms = []
            if "glossaryTerms" in entity and entity["glossaryTerms"]:
                glossary_terms = [
                    term["term"]["name"]
                    for term in entity["glossaryTerms"].get("terms", [])
                    if "term" in term and "name" in term["term"]
                ]

            platform = None
            if "platform" in entity and entity["platform"]:
                platform = entity["platform"].get("name")

            results.append(
                SearchResult(
                    urn=urn,
                    entity_type=entity_type,
                    name=entity.get("name"),
                    description=entity.get("description"),
                    platform=platform,
                    tags=tags,
                    glossary_terms=glossary_terms,
                    owners=[],
                )
            )

        facets = {}
        for facet in search_data.get("facets", []):
            field = facet.get("field", "")
            facets[field] = facet.get("aggregations", [])

        return SearchResponse(
            results=results,
            total=search_data.get("total", 0),
            page=search_data.get("start", 0) // max(search_data.get("count", 10), 1),
            page_size=search_data.get("count", 10),
            facets=facets,
        )

    def _parse_entity_response(
        self,
        urn: str,
        entity_type: str,
        response: dict[str, Any],
    ) -> EntityFull:
        """Parse OpenAPI batch get response into EntityFull model."""
        from datahub_mcp_server.models import EntityAspect

        aspects = []

        entities = response.get("entities", [])
        if entities and len(entities) > 0:
            entity = entities[0]

            for aspect_name, aspect_data in entity.get("aspects", {}).items():
                aspect_value = aspect_data.get("value", {})
                system_metadata = aspect_data.get("systemMetadata", {})

                aspects.append(
                    EntityAspect(
                        name=aspect_name,
                        value=aspect_value,
                        version=system_metadata.get("version"),
                        last_modified=system_metadata.get("lastObserved"),
                    )
                )

        return EntityFull(
            urn=urn,
            entity_type=entity_type,
            aspects=aspects,
        )


__all__ = ["DataHubTools"]
