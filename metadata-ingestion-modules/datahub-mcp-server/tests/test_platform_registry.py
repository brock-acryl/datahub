"""Tests for platform registry."""

from datahub_mcp_server.platform import PlatformRegistry


class TestPlatformRegistry:
    """Test platform registry."""

    def test_list_platforms(self) -> None:
        """Test listing supported platforms."""
        registry = PlatformRegistry()
        platforms = registry.list_platforms()

        assert len(platforms) > 0
        assert "snowflake" in platforms
        assert "bigquery" in platforms
        assert "mysql" in platforms

    def test_get_platform(self) -> None:
        """Test getting platform information."""
        registry = PlatformRegistry()

        snowflake = registry.get_platform("snowflake")
        assert snowflake is not None
        assert snowflake.platform_name == "snowflake"
        assert "dataset" in snowflake.entity_types
        assert snowflake.supports_platform_instance is True

    def test_get_platform_case_insensitive(self) -> None:
        """Test platform lookup is case-insensitive."""
        registry = PlatformRegistry()

        snowflake1 = registry.get_platform("snowflake")
        snowflake2 = registry.get_platform("SNOWFLAKE")
        snowflake3 = registry.get_platform("Snowflake")

        assert snowflake1 is not None
        assert snowflake2 is not None
        assert snowflake3 is not None
        assert snowflake1.platform_name == snowflake2.platform_name
        assert snowflake2.platform_name == snowflake3.platform_name

    def test_is_supported(self) -> None:
        """Test platform support check."""
        registry = PlatformRegistry()

        assert registry.is_supported("snowflake") is True
        assert registry.is_supported("bigquery") is True
        assert registry.is_supported("nonexistent") is False

    def test_validate_dataset_name(self) -> None:
        """Test dataset name validation."""
        registry = PlatformRegistry()

        assert registry.validate_dataset_name("snowflake", "db.schema.table") is True
        assert registry.validate_dataset_name("mysql", "db.table") is True
        assert registry.validate_dataset_name("bigquery", "project.dataset.table") is True

        assert registry.validate_dataset_name("snowflake", "db.schema") is True
        assert registry.validate_dataset_name("mysql", "table") is True
