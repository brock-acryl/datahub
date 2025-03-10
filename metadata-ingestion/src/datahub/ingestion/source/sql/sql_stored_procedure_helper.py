"""
Helper module for working with SQL stored procedures in DataHub.
This module provides utility functions to convert stored procedures to DataHub entities.
"""

from typing import Dict, List, Optional

from datahub.ingestion.source.sql.sql_generic import BaseStoredProcedure
from datahub.ingestion.source.sql.sql_job_models import (
    ProcedureDependency,
    ProcedureLineageStream,
    SQLDataFlow,
    SQLDataJob,
    SQLProceduresContainer,
    StoredProcedure,
)


def create_procedure_container(
    db_name: str,
    platform_instance: Optional[str],
    container_name: str,
    env: str,
    source: str,
) -> SQLProceduresContainer:
    """
    Create a container for stored procedures.

    Args:
        db_name: Database name
        platform_instance: Optional platform instance name
        container_name: Name for the container
        env: Environment name
        source: Source platform name

    Returns:
        A SQLProceduresContainer instance
    """
    return SQLProceduresContainer(
        db=db_name,
        platform_instance=platform_instance,
        name=container_name,
        env=env,
        source=source,
    )


def create_data_flow(
    container: SQLProceduresContainer,
    source: str,
    external_url: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
) -> SQLDataFlow:
    """
    Create a DataFlow entity for a stored procedures container.

    Args:
        container: The SQLProceduresContainer
        source: Source platform name
        external_url: Optional URL to external system
        properties: Optional custom properties

    Returns:
        A SQLDataFlow instance
    """
    data_flow = SQLDataFlow(
        entity=container,
        source=source,
        external_url=external_url,
    )

    if properties:
        for key, value in properties.items():
            data_flow.add_property(key, value)

    return data_flow


def convert_base_stored_procedure(
    base_proc: BaseStoredProcedure,
    db_name: str,
    container: SQLProceduresContainer,
    source: str,
) -> StoredProcedure:
    """
    Convert a BaseStoredProcedure to a StoredProcedure.

    Args:
        base_proc: The BaseStoredProcedure instance
        db_name: Database name
        container: The SQLProceduresContainer
        source: Source platform name

    Returns:
        A StoredProcedure instance
    """
    return base_proc.to_stored_procedure(db_name, container, source)


def create_data_job(
    stored_proc: StoredProcedure,
    source: str,
    description: Optional[str] = None,
    external_url: Optional[str] = None,
    status: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
) -> SQLDataJob:
    """
    Create a DataJob entity for a stored procedure.

    Args:
        stored_proc: The StoredProcedure instance
        source: Source platform name
        description: Optional description
        external_url: Optional URL to external system
        status: Optional status
        properties: Optional custom properties

    Returns:
        A SQLDataJob instance
    """
    data_job = SQLDataJob(
        entity=stored_proc,
        source=source,
        description=description,
        external_url=external_url,
        status=status,
    )

    if properties:
        for key, value in properties.items():
            data_job.add_property(key, value)

    return data_job


def add_lineage_to_data_job(
    data_job: SQLDataJob,
    input_datasets: List[str] = None,
    output_datasets: List[str] = None,
    input_jobs: List[str] = None,
) -> None:
    """
    Add lineage information to a DataJob.

    Args:
        data_job: The SQLDataJob instance
        input_datasets: Optional list of input dataset URNs
        output_datasets: Optional list of output dataset URNs
        input_jobs: Optional list of input job URNs
    """
    if input_datasets:
        data_job.incoming.extend(input_datasets)

    if output_datasets:
        data_job.outgoing.extend(output_datasets)

    if input_jobs:
        data_job.input_jobs.extend(input_jobs)


def create_procedure_dependency(
    db: str,
    schema: str,
    name: str,
    dependency_type: str,
    env: str,
    source: str,
    server: Optional[str] = None,
) -> ProcedureDependency:
    """
    Create a dependency for a stored procedure.

    Args:
        db: Database name
        schema: Schema name
        name: Object name
        dependency_type: Type of dependency (e.g., "TABLE", "VIEW")
        env: Environment name
        source: Source platform name
        server: Optional server name

    Returns:
        A ProcedureDependency instance
    """
    return ProcedureDependency(
        db=db,
        schema=schema,
        name=name,
        type=dependency_type,
        env=env,
        server=server,
        source=source,
    )


def create_procedure_lineage_stream(
    dependencies: List[ProcedureDependency],
) -> ProcedureLineageStream:
    """
    Create a lineage stream for a stored procedure.

    Args:
        dependencies: List of ProcedureDependency instances

    Returns:
        A ProcedureLineageStream instance
    """
    return ProcedureLineageStream(dependencies=dependencies)
