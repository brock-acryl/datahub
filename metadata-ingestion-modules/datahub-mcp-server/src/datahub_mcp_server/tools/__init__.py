"""MCP tools for DataHub operations."""

from typing import Any, Optional

from datahub_mcp_server.clients import GraphQLClient, OpenAPIClient
from datahub_mcp_server.entity_creator import EntityCreator
from datahub_mcp_server.models import (
    BulkOperationResult,
    ContainerCreateRequest,
    DatasetCreateRequest,
    EntityFull,
    EntityIdentifier,
    LineageResponse,
    SchemaField,
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
        self.entity_creator = EntityCreator(openapi_client, urn_resolver)

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

    async def bulk_add_tags(
        self,
        identifiers: list[EntityIdentifier],
        tag_names: list[str],
    ) -> BulkOperationResult:
        """Add tags to multiple entities using OpenAPI batch operations.

        Args:
            identifiers: List of entity identifiers
            tag_names: List of tag names to add to all entities

        Returns:
            BulkOperationResult with success/failure counts
        """
        tag_urns = [f"urn:li:tag:{tag}" for tag in tag_names]
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.add_tags(urn, tag_urns)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_remove_tags(
        self,
        identifiers: list[EntityIdentifier],
        tag_names: list[str],
    ) -> BulkOperationResult:
        """Remove tags from multiple entities.

        Args:
            identifiers: List of entity identifiers
            tag_names: List of tag names to remove from all entities

        Returns:
            BulkOperationResult with success/failure counts
        """
        tag_urns = [f"urn:li:tag:{tag}" for tag in tag_names]
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.remove_tags(urn, tag_urns)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_add_terms(
        self,
        identifiers: list[EntityIdentifier],
        term_names: list[str],
    ) -> BulkOperationResult:
        """Add glossary terms to multiple entities.

        Args:
            identifiers: List of entity identifiers
            term_names: List of glossary term names to add

        Returns:
            BulkOperationResult with success/failure counts
        """
        term_urns = [f"urn:li:glossaryTerm:{term}" for term in term_names]
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.add_terms(urn, term_urns)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_remove_terms(
        self,
        identifiers: list[EntityIdentifier],
        term_names: list[str],
    ) -> BulkOperationResult:
        """Remove glossary terms from multiple entities.

        Args:
            identifiers: List of entity identifiers
            term_names: List of glossary term names to remove

        Returns:
            BulkOperationResult with success/failure counts
        """
        term_urns = [f"urn:li:glossaryTerm:{term}" for term in term_names]
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.remove_terms(urn, term_urns)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_add_owners(
        self,
        identifiers: list[EntityIdentifier],
        owner_names: list[str],
        owner_type: str = "NONE",
    ) -> BulkOperationResult:
        """Add owners to multiple entities.

        Args:
            identifiers: List of entity identifiers
            owner_names: List of owner usernames to add
            owner_type: Owner type (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE)

        Returns:
            BulkOperationResult with success/failure counts
        """
        owner_urns = [f"urn:li:corpuser:{owner}" for owner in owner_names]
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.add_owners(urn, owner_urns, owner_type)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_remove_owners(
        self,
        identifiers: list[EntityIdentifier],
        owner_names: list[str],
    ) -> BulkOperationResult:
        """Remove owners from multiple entities.

        Args:
            identifiers: List of entity identifiers
            owner_names: List of owner usernames to remove

        Returns:
            BulkOperationResult with success/failure counts
        """
        owner_urns = [f"urn:li:corpuser:{owner}" for owner in owner_names]
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.remove_owners(urn, owner_urns)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_set_domain(
        self,
        identifiers: list[EntityIdentifier],
        domain_name: str,
    ) -> BulkOperationResult:
        """Set the domain for multiple entities.

        Args:
            identifiers: List of entity identifiers
            domain_name: Domain name to set

        Returns:
            BulkOperationResult with success/failure counts
        """
        domain_urn = f"urn:li:domain:{domain_name}"
        urns = [self.urn_resolver.resolve(identifier) for identifier in identifiers]

        success_count = 0
        failure_count = 0
        failures = []

        for urn in urns:
            try:
                await self.graphql_client.set_domain(urn, domain_urn)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def bulk_update_descriptions(
        self,
        updates: dict[str, str],
    ) -> BulkOperationResult:
        """Update descriptions for multiple entities.

        Args:
            updates: Dictionary mapping URNs to new descriptions

        Returns:
            BulkOperationResult with success/failure counts
        """
        success_count = 0
        failure_count = 0
        failures = []

        for urn, description in updates.items():
            try:
                await self.graphql_client.update_description(urn, description)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append({"urn": urn, "error": str(e)})

        return BulkOperationResult(
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

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

    async def create_dataset(
        self,
        platform: str,
        name: str,
        schema_fields: list[dict[str, Any]],
        env: str = "PROD",
        platform_instance: Optional[str] = None,
        description: str = "",
        container_urn: Optional[str] = None,
        subtype: str = "Table",
        external_url: Optional[str] = None,
        custom_properties: Optional[dict[str, str]] = None,
    ) -> dict[str, str]:
        """Create a new dataset entity in DataHub.

        Args:
            platform: Platform name (snowflake, bigquery, mysql, etc.)
            name: Dataset name (e.g., "db.schema.table" for Snowflake)
            schema_fields: List of field definitions with keys:
                - field_path: Field name
                - native_data_type: Native type (e.g., "VARCHAR(255)")
                - field_type: Type category (string, number, boolean, date, timestamp, bytes)
                - description: Optional field description
                - nullable: Optional boolean (default False)
                - is_part_of_key: Optional boolean (default False)
            env: Environment (PROD, DEV, etc.)
            platform_instance: Platform instance identifier
            description: Dataset description
            container_urn: Parent container URN (optional)
            subtype: Dataset subtype (Table, View, External)
            external_url: External URL to the dataset
            custom_properties: Custom properties dictionary

        Returns:
            {"urn": "urn:li:dataset:...", "status": "created"}
        """
        parsed_fields = [SchemaField(**field) for field in schema_fields]

        request = DatasetCreateRequest(
            platform=platform,
            name=name,
            env=env,
            platform_instance=platform_instance,
            description=description,
            schema_fields=parsed_fields,
            container_urn=container_urn,
            subtype=subtype,
            external_url=external_url,
            custom_properties=custom_properties or {},
        )

        urn = await self.entity_creator.create_dataset(request)

        return {"urn": urn, "status": "created"}

    async def create_container(
        self,
        platform: str,
        name: str,
        container_type: str,
        env: str = "PROD",
        description: str = "",
        parent_container_urn: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[dict[str, str]] = None,
    ) -> dict[str, str]:
        """Create a new container entity in DataHub.

        Args:
            platform: Platform name
            name: Container name
            container_type: Type of container (Database, Schema, Project, Dataset)
            env: Environment
            description: Container description
            parent_container_urn: Parent container URN for nested containers
            external_url: External URL to the container
            custom_properties: Custom properties dictionary

        Returns:
            {"urn": "urn:li:container:...", "status": "created"}
        """
        request = ContainerCreateRequest(
            platform=platform,
            name=name,
            env=env,
            container_type=container_type,
            description=description,
            parent_container_urn=parent_container_urn,
            external_url=external_url,
            custom_properties=custom_properties or {},
        )

        urn = await self.entity_creator.create_container(request)

        return {"urn": urn, "status": "created"}


__all__ = ["DataHubTools"]
