"""Platform registry for DataHub connectors.

This module maintains platform-specific metadata patterns derived from
DataHub connector golden test files.
"""

from typing import Optional


class PlatformInfo:
    """Information about a specific data platform."""

    def __init__(
        self,
        platform_name: str,
        entity_types: set[str],
        dataset_naming_pattern: str,
        supports_platform_instance: bool = False,
        default_aspects: Optional[list[str]] = None,
    ) -> None:
        self.platform_name = platform_name
        self.entity_types = entity_types
        self.dataset_naming_pattern = dataset_naming_pattern
        self.supports_platform_instance = supports_platform_instance
        self.default_aspects = default_aspects or []


class PlatformRegistry:
    """Registry of platform-specific metadata patterns.

    In a full implementation, this would be derived from parsing
    DataHub connector golden test files. For now, we provide a
    curated set of common platforms.
    """

    def __init__(self) -> None:
        self._platforms: dict[str, PlatformInfo] = {}
        self._initialize_platforms()

    def _initialize_platforms(self) -> None:
        """Initialize platform registry with common platforms."""
        self._platforms["snowflake"] = PlatformInfo(
            platform_name="snowflake",
            entity_types={"dataset", "dashboard", "chart"},
            dataset_naming_pattern="<database>.<schema>.<table>",
            supports_platform_instance=True,
            default_aspects=[
                "datasetProperties",
                "schemaMetadata",
                "ownership",
                "globalTags",
                "glossaryTerms",
            ],
        )

        self._platforms["bigquery"] = PlatformInfo(
            platform_name="bigquery",
            entity_types={"dataset"},
            dataset_naming_pattern="<project>.<dataset>.<table>",
            supports_platform_instance=True,
            default_aspects=[
                "datasetProperties",
                "schemaMetadata",
                "ownership",
                "globalTags",
                "glossaryTerms",
            ],
        )

        self._platforms["mysql"] = PlatformInfo(
            platform_name="mysql",
            entity_types={"dataset"},
            dataset_naming_pattern="<database>.<table>",
            supports_platform_instance=True,
            default_aspects=[
                "datasetProperties",
                "schemaMetadata",
                "ownership",
                "globalTags",
                "glossaryTerms",
            ],
        )

        self._platforms["postgres"] = PlatformInfo(
            platform_name="postgres",
            entity_types={"dataset"},
            dataset_naming_pattern="<database>.<schema>.<table>",
            supports_platform_instance=True,
            default_aspects=[
                "datasetProperties",
                "schemaMetadata",
                "ownership",
                "globalTags",
                "glossaryTerms",
            ],
        )

        self._platforms["redshift"] = PlatformInfo(
            platform_name="redshift",
            entity_types={"dataset"},
            dataset_naming_pattern="<database>.<schema>.<table>",
            supports_platform_instance=True,
            default_aspects=[
                "datasetProperties",
                "schemaMetadata",
                "ownership",
                "globalTags",
                "glossaryTerms",
            ],
        )

        self._platforms["tableau"] = PlatformInfo(
            platform_name="tableau",
            entity_types={"dashboard", "chart"},
            dataset_naming_pattern="<workbook>/<sheet>",
            supports_platform_instance=True,
            default_aspects=[
                "dashboardInfo",
                "chartInfo",
                "ownership",
                "globalTags",
            ],
        )

    def get_platform(self, platform_name: str) -> Optional[PlatformInfo]:
        """Get platform information by name."""
        return self._platforms.get(platform_name.lower())

    def is_supported(self, platform_name: str) -> bool:
        """Check if a platform is supported."""
        return platform_name.lower() in self._platforms

    def list_platforms(self) -> list[str]:
        """List all supported platforms."""
        return sorted(self._platforms.keys())

    def validate_dataset_name(self, platform_name: str, dataset_name: str) -> bool:
        """Validate a dataset name against platform conventions.

        Args:
            platform_name: Platform name
            dataset_name: Dataset name to validate

        Returns:
            True if name is valid for platform
        """
        platform = self.get_platform(platform_name)
        if not platform:
            return False

        pattern = platform.dataset_naming_pattern
        expected_parts = pattern.count(".") + 1
        actual_parts = dataset_name.count(".") + 1

        return actual_parts >= expected_parts - 1


__all__ = ["PlatformInfo", "PlatformRegistry"]
