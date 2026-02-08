"""Tests for entity creator."""

import pytest

from datahub_mcp_server.entity_creator import (
    build_browse_paths_v2_aspect,
    build_container_aspect,
    build_container_properties_aspect,
    build_data_platform_instance_aspect,
    build_dataset_properties_aspect,
    build_schema_field,
    build_schema_metadata_aspect,
    build_status_aspect,
    build_subtypes_aspect,
    generate_container_urn,
)
from datahub_mcp_server.models import (
    ContainerCreateRequest,
    DatasetCreateRequest,
    SchemaField,
)


class TestAspectBuilders:
    """Test aspect builder functions."""

    def test_build_status_aspect(self) -> None:
        """Test status aspect builder."""
        status = build_status_aspect()
        assert status == {"removed": False}

        status_removed = build_status_aspect(removed=True)
        assert status_removed == {"removed": True}

    def test_build_dataset_properties_aspect(self) -> None:
        """Test datasetProperties aspect builder."""
        request = DatasetCreateRequest(
            platform="snowflake",
            name="db.schema.table",
            description="Test dataset",
            external_url="https://example.com",
            custom_properties={"key1": "value1"},
        )

        properties = build_dataset_properties_aspect(request)

        assert properties["name"] == "table"
        assert properties["qualifiedName"] == "db.schema.table"
        assert properties["description"] == "Test dataset"
        assert properties["externalUrl"] == "https://example.com"
        assert properties["customProperties"]["key1"] == "value1"
        assert properties["tags"] == []

    def test_build_schema_field(self) -> None:
        """Test schema field builder."""
        field = SchemaField(
            field_path="col1",
            native_data_type="VARCHAR(255)",
            field_type="string",
            description="Test column",
            nullable=True,
            is_part_of_key=False,
        )

        field_json = build_schema_field(field)

        assert field_json["fieldPath"] == "col1"
        assert field_json["nativeDataType"] == "VARCHAR(255)"
        assert field_json["nullable"] is True
        assert field_json["description"] == "Test column"
        assert field_json["isPartOfKey"] is False
        assert field_json["recursive"] is False
        assert "type" in field_json

    def test_build_schema_metadata_aspect(self) -> None:
        """Test schemaMetadata aspect builder."""
        request = DatasetCreateRequest(
            platform="snowflake",
            name="db.schema.table",
            schema_fields=[
                SchemaField(
                    field_path="col1",
                    native_data_type="NUMBER(38,0)",
                    field_type="number",
                ),
                SchemaField(
                    field_path="col2",
                    native_data_type="VARCHAR(255)",
                    field_type="string",
                ),
            ],
        )

        schema_metadata = build_schema_metadata_aspect(request)

        assert schema_metadata["schemaName"] == "db.schema.table"
        assert schema_metadata["platform"] == "urn:li:dataPlatform:snowflake"
        assert schema_metadata["version"] == 0
        assert len(schema_metadata["fields"]) == 2
        assert schema_metadata["fields"][0]["fieldPath"] == "col1"
        assert schema_metadata["fields"][1]["fieldPath"] == "col2"

    def test_build_subtypes_aspect(self) -> None:
        """Test subTypes aspect builder."""
        subtypes = build_subtypes_aspect(["Table"])
        assert subtypes == {"typeNames": ["Table"]}

        subtypes_view = build_subtypes_aspect(["View"])
        assert subtypes_view == {"typeNames": ["View"]}

    def test_build_data_platform_instance_aspect(self) -> None:
        """Test dataPlatformInstance aspect builder."""
        dpi = build_data_platform_instance_aspect("snowflake")
        assert dpi == {"platform": "urn:li:dataPlatform:snowflake"}

    def test_build_browse_paths_v2_aspect(self) -> None:
        """Test browsePathsV2 aspect builder."""
        browse_paths = build_browse_paths_v2_aspect()
        assert browse_paths == {"path": []}

        parent_urn = "urn:li:container:abc123"
        browse_paths_with_parent = build_browse_paths_v2_aspect(
            [{"id": parent_urn, "urn": parent_urn}]
        )
        assert browse_paths_with_parent == {"path": [{"id": parent_urn, "urn": parent_urn}]}

    def test_build_container_aspect(self) -> None:
        """Test container aspect builder."""
        container_urn = "urn:li:container:abc123"
        container = build_container_aspect(container_urn)
        assert container == {"container": container_urn}

    def test_build_container_properties_aspect(self) -> None:
        """Test containerProperties aspect builder."""
        request = ContainerCreateRequest(
            platform="snowflake",
            name="test_db",
            container_type="Database",
            env="PROD",
            description="Test database",
            external_url="https://example.com",
            custom_properties={"key1": "value1"},
        )

        properties = build_container_properties_aspect(request)

        assert properties["name"] == "test_db"
        assert properties["env"] == "PROD"
        assert properties["description"] == "Test database"
        assert properties["externalUrl"] == "https://example.com"
        assert properties["customProperties"]["platform"] == "snowflake"
        assert properties["customProperties"]["env"] == "PROD"
        assert properties["customProperties"]["database"] == "test_db"
        assert properties["customProperties"]["key1"] == "value1"


class TestContainerUrnGeneration:
    """Test container URN generation."""

    def test_generate_container_urn(self) -> None:
        """Test container URN generation with MD5 hash."""
        urn = generate_container_urn("snowflake", "test_db", "PROD", "Database")

        assert urn.startswith("urn:li:container:")
        hash_part = urn.replace("urn:li:container:", "")
        assert len(hash_part) == 32

    def test_generate_container_urn_consistent(self) -> None:
        """Test that same inputs produce same URN."""
        urn1 = generate_container_urn("snowflake", "test_db", "PROD", "Database")
        urn2 = generate_container_urn("snowflake", "test_db", "PROD", "Database")

        assert urn1 == urn2

    def test_generate_container_urn_different_inputs(self) -> None:
        """Test that different inputs produce different URNs."""
        urn1 = generate_container_urn("snowflake", "test_db", "PROD", "Database")
        urn2 = generate_container_urn("snowflake", "test_db", "DEV", "Database")
        urn3 = generate_container_urn("snowflake", "other_db", "PROD", "Database")

        assert urn1 != urn2
        assert urn1 != urn3
        assert urn2 != urn3
