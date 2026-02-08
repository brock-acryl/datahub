"""Entity creation with platform-specific patterns."""

import hashlib
from typing import Any, Optional

from datahub_mcp_server.clients import OpenAPIClient
from datahub_mcp_server.models import (
    ContainerCreateRequest,
    DatasetCreateRequest,
    SchemaField,
)
from datahub_mcp_server.utils import UrnResolver

TYPE_MAPPING: dict[str, dict[str, Any]] = {
    "string": {"type": {"com.linkedin.schema.StringType": {}}},
    "number": {"type": {"com.linkedin.schema.NumberType": {}}},
    "boolean": {"type": {"com.linkedin.schema.BooleanType": {}}},
    "date": {"type": {"com.linkedin.schema.DateType": {}}},
    "timestamp": {"type": {"com.linkedin.schema.TimeType": {}}},
    "bytes": {"type": {"com.linkedin.schema.BytesType": {}}},
    "array": {"type": {"com.linkedin.schema.ArrayType": {}}},
    "record": {"type": {"com.linkedin.schema.RecordType": {}}},
}


def build_status_aspect(removed: bool = False) -> dict[str, Any]:
    """Build status aspect."""
    return {"removed": removed}


def build_dataset_properties_aspect(request: DatasetCreateRequest) -> dict[str, Any]:
    """Build datasetProperties aspect."""
    properties = {
        "name": request.name.split(".")[-1],
        "qualifiedName": request.name,
        "customProperties": request.custom_properties.copy(),
        "tags": [],
    }

    if request.description:
        properties["description"] = request.description

    if request.external_url:
        properties["externalUrl"] = request.external_url

    return properties


def build_schema_field(field: SchemaField) -> dict[str, Any]:
    """Build schema field JSON."""
    field_json: dict[str, Any] = {
        "fieldPath": field.field_path,
        "nullable": field.nullable,
        "type": TYPE_MAPPING.get(field.field_type, TYPE_MAPPING["string"]),
        "nativeDataType": field.native_data_type,
        "recursive": False,
        "isPartOfKey": field.is_part_of_key,
    }

    if field.description:
        field_json["description"] = field.description

    return field_json


def build_schema_metadata_aspect(request: DatasetCreateRequest) -> dict[str, Any]:
    """Build schemaMetadata aspect."""
    return {
        "schemaName": request.name,
        "platform": f"urn:li:dataPlatform:{request.platform}",
        "version": 0,
        "created": {"time": 0, "actor": "urn:li:corpuser:unknown"},
        "lastModified": {"time": 0, "actor": "urn:li:corpuser:unknown"},
        "hash": "",
        "platformSchema": {"com.linkedin.schema.MySqlDDL": {"tableSchema": ""}},
        "fields": [build_schema_field(field) for field in request.schema_fields],
    }


def build_subtypes_aspect(type_names: list[str]) -> dict[str, Any]:
    """Build subTypes aspect."""
    return {"typeNames": type_names}


def build_data_platform_instance_aspect(platform: str) -> dict[str, Any]:
    """Build dataPlatformInstance aspect."""
    return {"platform": f"urn:li:dataPlatform:{platform}"}


def build_browse_paths_v2_aspect(
    path: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    """Build browsePathsV2 aspect."""
    if path is None:
        path = []
    return {"path": path}


def build_container_aspect(container_urn: str) -> dict[str, Any]:
    """Build container aspect."""
    return {"container": container_urn}


def build_container_properties_aspect(request: ContainerCreateRequest) -> dict[str, Any]:
    """Build containerProperties aspect."""
    custom_props = request.custom_properties.copy()
    custom_props["platform"] = request.platform
    custom_props["env"] = request.env

    if request.container_type.lower() == "database":
        custom_props["database"] = request.name
    elif request.container_type.lower() == "schema":
        custom_props["schema"] = request.name

    properties: dict[str, Any] = {
        "name": request.name,
        "customProperties": custom_props,
        "env": request.env,
    }

    if request.description:
        properties["description"] = request.description

    if request.external_url:
        properties["externalUrl"] = request.external_url

    return properties


def generate_container_urn(platform: str, name: str, env: str, container_type: str) -> str:
    """Generate container URN using MD5 hash."""
    key = f"{platform}.{container_type}.{name}.{env}".lower()
    hash_value = hashlib.md5(key.encode()).hexdigest()
    return f"urn:li:container:{hash_value}"


class EntityCreator:
    """Create entities with platform-specific patterns."""

    def __init__(self, openapi_client: OpenAPIClient, urn_resolver: UrnResolver):
        """Initialize entity creator."""
        self.client = openapi_client
        self.urn_resolver = urn_resolver

    async def create_dataset(self, request: DatasetCreateRequest) -> str:
        """Create a dataset entity with all required aspects.

        Args:
            request: Dataset creation request

        Returns:
            Dataset URN
        """
        from datahub_mcp_server.models import EntityIdentifier

        identifier = EntityIdentifier(
            entity_type="dataset",
            platform=request.platform,
            platform_instance=request.platform_instance,
            name=request.name,
            env=request.env,
        )
        urn = self.urn_resolver.resolve(identifier)

        aspects_to_create = [
            ("status", build_status_aspect()),
            ("datasetProperties", build_dataset_properties_aspect(request)),
            ("subTypes", build_subtypes_aspect([request.subtype])),
            ("dataPlatformInstance", build_data_platform_instance_aspect(request.platform)),
        ]

        if request.schema_fields:
            aspects_to_create.append(("schemaMetadata", build_schema_metadata_aspect(request)))

        if request.container_urn:
            aspects_to_create.append(("container", build_container_aspect(request.container_urn)))

            parent_path = [{"id": request.container_urn, "urn": request.container_urn}]
            aspects_to_create.append(("browsePathsV2", build_browse_paths_v2_aspect(parent_path)))
        else:
            aspects_to_create.append(("browsePathsV2", build_browse_paths_v2_aspect()))

        for aspect_name, aspect_value in aspects_to_create:
            await self.client.create_aspect(
                entity_type="dataset",
                urn=urn,
                aspect_name=aspect_name,
                aspect_value=aspect_value,
            )

        return urn

    async def create_container(self, request: ContainerCreateRequest) -> str:
        """Create a container entity with all required aspects.

        Args:
            request: Container creation request

        Returns:
            Container URN
        """
        urn = generate_container_urn(
            request.platform,
            request.name,
            request.env,
            request.container_type,
        )

        aspects_to_create = [
            ("containerProperties", build_container_properties_aspect(request)),
            ("status", build_status_aspect()),
            ("dataPlatformInstance", build_data_platform_instance_aspect(request.platform)),
            ("subTypes", build_subtypes_aspect([request.container_type])),
        ]

        if request.parent_container_urn:
            aspects_to_create.append(("container", build_container_aspect(request.parent_container_urn)))

            parent_path = [{"id": request.parent_container_urn, "urn": request.parent_container_urn}]
            aspects_to_create.append(("browsePathsV2", build_browse_paths_v2_aspect(parent_path)))
        else:
            aspects_to_create.append(("browsePathsV2", build_browse_paths_v2_aspect()))

        for aspect_name, aspect_value in aspects_to_create:
            await self.client.create_aspect(
                entity_type="container",
                urn=urn,
                aspect_name=aspect_name,
                aspect_value=aspect_value,
            )

        return urn
