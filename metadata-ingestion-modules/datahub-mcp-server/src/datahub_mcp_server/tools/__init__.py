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

    async def add_tags(
        self,
        identifier: EntityIdentifier,
        tag_names: list[str],
    ) -> bool:
        """Add tags to an entity.

        Args:
            identifier: Entity identifier
            tag_names: List of tag names (not URNs) to add

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        tag_urns = [f"urn:li:tag:{tag}" for tag in tag_names]

        await self.graphql_client.add_tags(urn, tag_urns)
        return True

    async def remove_tags(
        self,
        identifier: EntityIdentifier,
        tag_names: list[str],
    ) -> bool:
        """Remove tags from an entity.

        Args:
            identifier: Entity identifier
            tag_names: List of tag names (not URNs) to remove

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        tag_urns = [f"urn:li:tag:{tag}" for tag in tag_names]

        await self.graphql_client.remove_tags(urn, tag_urns)
        return True

    async def add_terms(
        self,
        identifier: EntityIdentifier,
        term_names: list[str],
    ) -> bool:
        """Add glossary terms to an entity.

        Args:
            identifier: Entity identifier
            term_names: List of term names (not URNs) to add

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        term_urns = [f"urn:li:glossaryTerm:{term}" for term in term_names]

        await self.graphql_client.add_terms(urn, term_urns)
        return True

    async def remove_terms(
        self,
        identifier: EntityIdentifier,
        term_names: list[str],
    ) -> bool:
        """Remove glossary terms from an entity.

        Args:
            identifier: Entity identifier
            term_names: List of term names (not URNs) to remove

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        term_urns = [f"urn:li:glossaryTerm:{term}" for term in term_names]

        await self.graphql_client.remove_terms(urn, term_urns)
        return True

    async def add_owners(
        self,
        identifier: EntityIdentifier,
        owner_names: list[str],
        owner_type: str = "NONE",
    ) -> bool:
        """Add owners to an entity.

        Args:
            identifier: Entity identifier
            owner_names: List of owner usernames
            owner_type: Owner type (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE)

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        owner_urns = [f"urn:li:corpuser:{owner}" for owner in owner_names]

        await self.graphql_client.add_owners(urn, owner_urns, owner_type)
        return True

    async def remove_owners(
        self,
        identifier: EntityIdentifier,
        owner_names: list[str],
    ) -> bool:
        """Remove owners from an entity.

        Args:
            identifier: Entity identifier
            owner_names: List of owner usernames to remove

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        owner_urns = [f"urn:li:corpuser:{owner}" for owner in owner_names]

        await self.graphql_client.remove_owners(urn, owner_urns)
        return True

    async def update_description(
        self,
        identifier: EntityIdentifier,
        description: str,
    ) -> bool:
        """Update entity description.

        Args:
            identifier: Entity identifier
            description: New description text

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)

        await self.graphql_client.update_description(urn, description)
        return True

    async def set_domain(
        self,
        identifier: EntityIdentifier,
        domain_name: str,
    ) -> bool:
        """Set the domain for an entity.

        Args:
            identifier: Entity identifier
            domain_name: Domain name (not URN)

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)
        domain_urn = f"urn:li:domain:{domain_name}"

        await self.graphql_client.set_domain(urn, domain_urn)
        return True

    async def unset_domain(
        self,
        identifier: EntityIdentifier,
    ) -> bool:
        """Unset the domain for an entity.

        Args:
            identifier: Entity identifier

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)

        await self.graphql_client.unset_domain(urn)
        return True

    async def update_deprecation(
        self,
        identifier: EntityIdentifier,
        deprecated: bool,
        note: Optional[str] = None,
        decommission_time: Optional[int] = None,
    ) -> bool:
        """Update deprecation status for an entity.

        Args:
            identifier: Entity identifier
            deprecated: Whether the entity is deprecated
            note: Optional deprecation note
            decommission_time: Optional decommission timestamp (milliseconds)

        Returns:
            True if successful
        """
        urn = self.urn_resolver.resolve(identifier)

        await self.graphql_client.update_deprecation(urn, deprecated, note, decommission_time)
        return True

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
