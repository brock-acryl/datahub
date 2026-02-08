"""Utilities for DataHub MCP Server."""

from datahub_mcp_server.models import EntityIdentifier


class UrnResolver:
    """Resolves entity identifiers to DataHub URNs.

    Handles platform-specific URN conventions and validation.
    """

    ENTITY_TYPES = {
        "dataset",
        "dashboard",
        "chart",
        "dataJob",
        "dataFlow",
        "container",
        "domain",
        "tag",
        "glossaryTerm",
        "corpuser",
        "corpGroup",
        "mlModel",
        "mlFeature",
        "mlFeatureTable",
        "mlPrimaryKey",
    }

    def __init__(self, default_env: str = "PROD") -> None:
        self.default_env = default_env

    def resolve(self, identifier: EntityIdentifier) -> str:
        """Resolve an EntityIdentifier to a DataHub URN.

        Args:
            identifier: Entity identifier (URN or human-friendly format)

        Returns:
            Validated DataHub URN

        Raises:
            ValueError: If identifier is invalid or cannot be resolved
        """
        if identifier.urn:
            return self._validate_urn(identifier.urn)

        if identifier.entity_type == "dataset":
            return self._resolve_dataset_urn(identifier)
        elif identifier.entity_type in self.ENTITY_TYPES:
            raise NotImplementedError(f"URN resolution for {identifier.entity_type} not yet implemented")
        else:
            raise ValueError(f"Unknown entity type: {identifier.entity_type}")

    def _validate_urn(self, urn: str) -> str:
        """Validate a URN format."""
        if not urn.startswith("urn:li:"):
            raise ValueError(f"Invalid URN format: {urn}")
        return urn

    def _resolve_dataset_urn(self, identifier: EntityIdentifier) -> str:
        """Resolve a dataset identifier to a URN.

        Format: urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)
        """
        if not identifier.platform:
            raise ValueError("Platform is required for dataset URN resolution")
        if not identifier.name:
            raise ValueError("Name is required for dataset URN resolution")

        env = identifier.env or self.default_env
        platform_urn = f"urn:li:dataPlatform:{identifier.platform}"

        if identifier.platform_instance:
            dataset_key = f"{platform_urn},{identifier.platform_instance}.{identifier.name},{env}"
        else:
            dataset_key = f"{platform_urn},{identifier.name},{env}"

        return f"urn:li:dataset:({dataset_key})"

    def parse_urn(self, urn: str) -> tuple[str, dict[str, str]]:
        """Parse a DataHub URN into entity type and components.

        Args:
            urn: DataHub URN

        Returns:
            Tuple of (entity_type, components_dict)

        Raises:
            ValueError: If URN format is invalid
        """
        if not urn.startswith("urn:li:"):
            raise ValueError(f"Invalid URN format: {urn}")

        parts = urn.split(":", 3)
        if len(parts) < 4:
            raise ValueError(f"Invalid URN format: {urn}")

        entity_type = parts[2]
        key = parts[3]

        if entity_type == "dataset":
            return entity_type, self._parse_dataset_key(key)
        else:
            return entity_type, {"key": key}

    def _parse_dataset_key(self, key: str) -> dict[str, str]:
        """Parse dataset URN key into components.

        Expected format: (urn:li:dataPlatform:<platform>,<name>,<env>)
        """
        if not (key.startswith("(") and key.endswith(")")):
            raise ValueError(f"Invalid dataset key format: {key}")

        key_inner = key[1:-1]
        parts = key_inner.split(",")

        if len(parts) < 3:
            raise ValueError(f"Invalid dataset key format: {key}")

        platform_urn = parts[0]
        env = parts[-1]
        name = ",".join(parts[1:-1])

        if not platform_urn.startswith("urn:li:dataPlatform:"):
            raise ValueError(f"Invalid platform URN: {platform_urn}")

        platform = platform_urn.split(":")[-1]

        components = {
            "platform": platform,
            "name": name,
            "env": env,
        }

        return components


class EntityNormalizer:
    """Normalizes entity representations into canonical shapes.

    Provides stable, LLM-friendly entity representations across different
    API responses and platform conventions.
    """

    def normalize_entity(self, raw_entity: dict) -> dict:
        """Normalize a raw entity into canonical form."""
        raise NotImplementedError("Entity normalization coming in next phase")


__all__ = ["UrnResolver", "EntityNormalizer"]
