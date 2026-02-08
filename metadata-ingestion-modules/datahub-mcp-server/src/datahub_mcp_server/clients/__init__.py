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


__all__ = ["BaseClient", "OpenAPIClient", "GraphQLClient"]
