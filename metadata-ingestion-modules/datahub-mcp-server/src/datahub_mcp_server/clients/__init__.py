"""API clients for DataHub."""

from typing import Any, Optional

import aiohttp

from datahub_mcp_server.config import DataHubConfig


class BaseClient:
    """Base client for DataHub API calls."""

    def __init__(self, config: DataHubConfig) -> None:
        self.config = config
        self.base_url = config.url.rstrip("/")
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None:
            headers = {}
            if self.config.token:
                headers["Authorization"] = f"Bearer {self.config.token}"

            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
            )
        return self.session

    async def close(self) -> None:
        """Close the client session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make an HTTP request with retries."""
        session = await self._get_session()

        for attempt in range(self.config.max_retries):
            try:
                async with session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                if attempt == self.config.max_retries - 1:
                    raise
                await self._handle_retry(e, attempt)

        raise RuntimeError("Max retries exceeded")

    async def _handle_retry(self, error: Exception, attempt: int) -> None:
        """Handle retry logic."""
        import asyncio

        wait_time = 2**attempt
        await asyncio.sleep(wait_time)


class OpenAPIClient(BaseClient):
    """Client for DataHub OpenAPI endpoints.

    Provides access to batch operations, generic patching, and
    conditional writes.
    """

    async def batch_get_entities(
        self,
        entity_type: str,
        urns: list[str],
        aspects: Optional[list[str]] = None,
        include_system_metadata: bool = True,
    ) -> dict[str, Any]:
        """Batch get entities with specified aspects.

        Args:
            entity_type: Entity type (dataset, dashboard, etc.)
            urns: List of entity URNs
            aspects: Optional list of aspect names (None = all aspects)
            include_system_metadata: Include system metadata with versions

        Returns:
            Batch get response with entities and aspects
        """
        url = f"{self.base_url}/openapi/v3/entity/{entity_type}/batchGet"

        params: dict[str, Any] = {
            "urns": urns,
            "systemMetadata": include_system_metadata,
        }

        if aspects:
            params["aspects"] = aspects

        return await self._request("GET", url, params=params)

    async def generic_patch(
        self,
        entity_type: str,
        urn: str,
        aspect_name: str,
        patch_operations: list[dict[str, Any]],
        if_version_match: Optional[str] = None,
    ) -> dict[str, Any]:
        """Apply a generic JSON Patch to an entity aspect.

        Args:
            entity_type: Entity type
            urn: Entity URN
            aspect_name: Aspect name to patch
            patch_operations: List of patch operations
            if_version_match: Optional version for conditional write

        Returns:
            Patch response
        """
        url = f"{self.base_url}/openapi/v3/entity/{entity_type}/{urn}/{aspect_name}"

        headers = {}
        if if_version_match:
            headers["If-Version-Match"] = if_version_match

        return await self._request(
            "PATCH",
            url,
            json=patch_operations,
            headers=headers,
        )

    async def create_aspect(
        self,
        entity_type: str,
        urn: str,
        aspect_name: str,
        aspect_value: dict[str, Any],
        if_version_match: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create or update an entity aspect.

        Args:
            entity_type: Entity type
            urn: Entity URN
            aspect_name: Aspect name
            aspect_value: Aspect value
            if_version_match: Optional version for conditional write

        Returns:
            Create response
        """
        url = f"{self.base_url}/openapi/v3/entity/{entity_type}/{urn}/{aspect_name}"

        payload: dict[str, Any] = {"value": aspect_value}

        if if_version_match:
            payload["headers"] = {"If-Version-Match": if_version_match}

        return await self._request("POST", url, json=payload)


class GraphQLClient(BaseClient):
    """Client for DataHub GraphQL API.

    Provides UI-parity operations for search, mutations, and
    interactive queries.
    """

    async def execute(
        self,
        query: str,
        variables: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query.

        Args:
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            GraphQL response
        """
        url = f"{self.base_url}/api/graphql"

        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = await self._request("POST", url, json=payload)

        if "errors" in response:
            raise RuntimeError(f"GraphQL errors: {response['errors']}")

        return response.get("data", {})

    async def search(
        self,
        query: str,
        types: Optional[list[str]] = None,
        filters: Optional[list[dict[str, Any]]] = None,
        start: int = 0,
        count: int = 10,
    ) -> dict[str, Any]:
        """Search for entities.

        Args:
            query: Search query string
            types: Optional list of entity types to filter
            filters: Optional list of search filters
            start: Result offset
            count: Number of results to return

        Returns:
            Search response with results
        """
        graphql_query = """
        query search($input: SearchInput!) {
            search(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            description
                            platform {
                                name
                            }
                            tags {
                                tags {
                                    tag {
                                        urn
                                        name
                                    }
                                }
                            }
                            glossaryTerms {
                                terms {
                                    term {
                                        urn
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
                facets {
                    field
                    aggregations {
                        value
                        count
                    }
                }
            }
        }
        """

        variables = {
            "input": {
                "type": types[0] if types and len(types) == 1 else "DATASET",
                "query": query,
                "start": start,
                "count": count,
            }
        }

        if filters:
            variables["input"]["filters"] = filters

        return await self.execute(graphql_query, variables)

    async def add_tags(
        self,
        urn: str,
        tag_urns: list[str],
    ) -> dict[str, Any]:
        """Add tags to an entity.

        Args:
            urn: Entity URN
            tag_urns: List of tag URNs to add

        Returns:
            Mutation response
        """
        mutation = """
        mutation addTags($input: BatchAddTagsInput!) {
            addTags(input: $input)
        }
        """

        variables = {
            "input": {
                "tagUrns": tag_urns,
                "resources": [{"resourceUrn": urn}],
            }
        }

        return await self.execute(mutation, variables)

    async def remove_tags(
        self,
        urn: str,
        tag_urns: list[str],
    ) -> dict[str, Any]:
        """Remove tags from an entity.

        Args:
            urn: Entity URN
            tag_urns: List of tag URNs to remove

        Returns:
            Mutation response
        """
        mutation = """
        mutation removeTags($input: BatchRemoveTagsInput!) {
            removeTags(input: $input)
        }
        """

        variables = {
            "input": {
                "tagUrns": tag_urns,
                "resources": [{"resourceUrn": urn}],
            }
        }

        return await self.execute(mutation, variables)

    async def add_terms(
        self,
        urn: str,
        term_urns: list[str],
    ) -> dict[str, Any]:
        """Add glossary terms to an entity.

        Args:
            urn: Entity URN
            term_urns: List of glossary term URNs to add

        Returns:
            Mutation response
        """
        mutation = """
        mutation addTerms($input: BatchAddTermsInput!) {
            addTerms(input: $input)
        }
        """

        variables = {
            "input": {
                "termUrns": term_urns,
                "resources": [{"resourceUrn": urn}],
            }
        }

        return await self.execute(mutation, variables)

    async def remove_terms(
        self,
        urn: str,
        term_urns: list[str],
    ) -> dict[str, Any]:
        """Remove glossary terms from an entity.

        Args:
            urn: Entity URN
            term_urns: List of glossary term URNs to remove

        Returns:
            Mutation response
        """
        mutation = """
        mutation removeTerms($input: BatchRemoveTermsInput!) {
            removeTerms(input: $input)
        }
        """

        variables = {
            "input": {
                "termUrns": term_urns,
                "resources": [{"resourceUrn": urn}],
            }
        }

        return await self.execute(mutation, variables)

    async def add_owners(
        self,
        urn: str,
        owner_urns: list[str],
        owner_type: str = "NONE",
    ) -> dict[str, Any]:
        """Add owners to an entity.

        Args:
            urn: Entity URN
            owner_urns: List of owner URNs (corpuser or corpGroup)
            owner_type: Owner type (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE)

        Returns:
            Mutation response
        """
        mutation = """
        mutation addOwners($input: AddOwnersInput!) {
            addOwners(input: $input)
        }
        """

        owners = [{"ownerUrn": owner_urn, "type": owner_type} for owner_urn in owner_urns]

        variables = {
            "input": {
                "owners": owners,
                "resourceUrn": urn,
            }
        }

        return await self.execute(mutation, variables)

    async def remove_owners(
        self,
        urn: str,
        owner_urns: list[str],
    ) -> dict[str, Any]:
        """Remove owners from an entity.

        Args:
            urn: Entity URN
            owner_urns: List of owner URNs to remove

        Returns:
            Mutation response
        """
        mutation = """
        mutation removeOwners($input: RemoveOwnersInput!) {
            removeOwners(input: $input)
        }
        """

        variables = {
            "input": {
                "ownerUrns": owner_urns,
                "resourceUrn": urn,
            }
        }

        return await self.execute(mutation, variables)

    async def update_description(
        self,
        urn: str,
        description: str,
    ) -> dict[str, Any]:
        """Update entity description.

        Args:
            urn: Entity URN
            description: New description text

        Returns:
            Mutation response
        """
        mutation = """
        mutation updateDescription($input: DescriptionUpdateInput!) {
            updateDescription(input: $input)
        }
        """

        variables = {
            "input": {
                "description": description,
                "resourceUrn": urn,
            }
        }

        return await self.execute(mutation, variables)

    async def set_domain(
        self,
        urn: str,
        domain_urn: str,
    ) -> dict[str, Any]:
        """Set the domain for an entity.

        Args:
            urn: Entity URN
            domain_urn: Domain URN

        Returns:
            Mutation response
        """
        mutation = """
        mutation setDomain($input: SetDomainInput!) {
            setDomain(input: $input)
        }
        """

        variables = {
            "input": {
                "domainUrn": domain_urn,
                "resourceUrn": urn,
            }
        }

        return await self.execute(mutation, variables)

    async def unset_domain(
        self,
        urn: str,
    ) -> dict[str, Any]:
        """Unset the domain for an entity.

        Args:
            urn: Entity URN

        Returns:
            Mutation response
        """
        mutation = """
        mutation unsetDomain($input: UnsetDomainInput!) {
            unsetDomain(input: $input)
        }
        """

        variables = {
            "input": {
                "resourceUrn": urn,
            }
        }

        return await self.execute(mutation, variables)

    async def update_deprecation(
        self,
        urn: str,
        deprecated: bool,
        note: Optional[str] = None,
        decommission_time: Optional[int] = None,
    ) -> dict[str, Any]:
        """Update deprecation status for an entity.

        Args:
            urn: Entity URN
            deprecated: Whether the entity is deprecated
            note: Optional deprecation note
            decommission_time: Optional decommission timestamp (milliseconds)

        Returns:
            Mutation response
        """
        mutation = """
        mutation updateDeprecation($input: UpdateDeprecationInput!) {
            updateDeprecation(input: $input)
        }
        """

        variables = {
            "input": {
                "urn": urn,
                "deprecated": deprecated,
            }
        }

        if note:
            variables["input"]["note"] = note
        if decommission_time:
            variables["input"]["decommissionTime"] = decommission_time

        return await self.execute(mutation, variables)


__all__ = ["BaseClient", "OpenAPIClient", "GraphQLClient"]
