"""Tests for URN resolution."""

import pytest

from datahub_mcp_server.models import EntityIdentifier
from datahub_mcp_server.utils import UrnResolver


class TestUrnResolver:
    """Test URN resolution and validation."""

    def test_validate_urn(self) -> None:
        """Test URN validation."""
        resolver = UrnResolver()

        valid_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        assert resolver.resolve(EntityIdentifier(urn=valid_urn)) == valid_urn

    def test_validate_invalid_urn(self) -> None:
        """Test invalid URN format."""
        resolver = UrnResolver()

        with pytest.raises(ValueError, match="Invalid URN format"):
            resolver.resolve(EntityIdentifier(urn="invalid:urn"))

    def test_resolve_dataset_urn(self) -> None:
        """Test dataset URN resolution."""
        resolver = UrnResolver()

        identifier = EntityIdentifier(
            entity_type="dataset",
            platform="snowflake",
            name="db.schema.table",
            env="PROD",
        )

        urn = resolver.resolve(identifier)
        expected = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        assert urn == expected

    def test_resolve_dataset_with_instance(self) -> None:
        """Test dataset URN resolution with platform instance."""
        resolver = UrnResolver()

        identifier = EntityIdentifier(
            entity_type="dataset",
            platform="snowflake",
            platform_instance="prod-instance",
            name="db.schema.table",
            env="PROD",
        )

        urn = resolver.resolve(identifier)
        expected = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod-instance.db.schema.table,PROD)"
        assert urn == expected

    def test_parse_dataset_urn(self) -> None:
        """Test dataset URN parsing."""
        resolver = UrnResolver()

        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        entity_type, components = resolver.parse_urn(urn)

        assert entity_type == "dataset"
        assert components["platform"] == "snowflake"
        assert components["name"] == "db.schema.table"
        assert components["env"] == "PROD"

    def test_parse_dataset_urn_with_instance(self) -> None:
        """Test dataset URN parsing with platform instance.

        Note: Platform instances become part of the name in the URN,
        and cannot be reliably parsed back out without additional context.
        """
        resolver = UrnResolver()

        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod-instance.db.schema.table,PROD)"
        entity_type, components = resolver.parse_urn(urn)

        assert entity_type == "dataset"
        assert components["platform"] == "snowflake"
        assert components["name"] == "prod-instance.db.schema.table"
        assert components["env"] == "PROD"

    def test_missing_platform(self) -> None:
        """Test error when platform is missing."""
        resolver = UrnResolver()

        identifier = EntityIdentifier(
            entity_type="dataset",
            name="db.schema.table",
        )

        with pytest.raises(ValueError, match="Platform is required"):
            resolver.resolve(identifier)

    def test_missing_name(self) -> None:
        """Test error when name is missing."""
        resolver = UrnResolver()

        identifier = EntityIdentifier(
            entity_type="dataset",
            platform="snowflake",
        )

        with pytest.raises(ValueError, match="Name is required"):
            resolver.resolve(identifier)
