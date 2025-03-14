"""
Helper module for working with SQL stored procedures in DataHub.
This module provides utility functions to convert stored procedures to DataHub entities.
"""

import logging
from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.sql_generic import BaseStoredProcedure
from datahub.ingestion.source.sql.sql_job_models import (
    ProcedureDependency,
    ProcedureLineageStream,
    SQLDataFlow,
    SQLDataJob,
    SQLProceduresContainer,
    StoredProcedure,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.datajob import to_datajob_input_output
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)

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


def parse_procedure_code(
    *,
    schema_resolver: SchemaResolver,
    default_db: Optional[str],
    default_schema: Optional[str],
    code: str,
    is_temp_table: Callable[[str], bool],
    raise_: bool = False,
) -> Optional[DataJobInputOutputClass]:
    """
    Parse stored procedure code to extract lineage information.
    
    Args:
        schema_resolver: SchemaResolver instance for resolving table references
        default_db: Default database name
        default_schema: Default schema name
        code: The stored procedure code to parse
        is_temp_table: Function to determine if a table is temporary
        raise_: Whether to raise exceptions on parsing failures
        
    Returns:
        Optional DataJobInputOutputClass containing lineage information
    """
    aggregator = SqlParsingAggregator(
        platform=schema_resolver.platform,
        env=schema_resolver.env,
        schema_resolver=schema_resolver,
        generate_lineage=True,
        generate_queries=False,
        generate_usage_statistics=False,
        generate_operations=False,
        generate_query_subject_fields=False,
        generate_query_usage_statistics=False,
        is_temp_table=is_temp_table,
    )
    
    for query in split_statements(code):
        # TODO: We should take into account `USE x` statements.
        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db,
                default_schema=default_schema,
                query=query,
            )
        )
    
    if aggregator.report.num_observed_queries_failed and raise_:
        logger.info(aggregator.report.as_string())
        raise ValueError(
            f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
        )

    mcps = list(aggregator.gen_metadata())
    return to_datajob_input_output(
        mcps=mcps,
        ignore_extra_mcps=True,
    )


def generate_procedure_lineage(
    *,
    schema_resolver: SchemaResolver,
    procedure: StoredProcedure,
    procedure_job_urn: str,
    is_temp_table: Callable[[str], bool] = lambda _: False,
    raise_: bool = False,
) -> Iterable[MetadataChangeProposalWrapper]:
    """
    Generate lineage metadata for a stored procedure.
    
    Args:
        schema_resolver: SchemaResolver instance for resolving table references
        procedure: The StoredProcedure instance
        procedure_job_urn: URN for the procedure job
        is_temp_table: Function to determine if a table is temporary
        raise_: Whether to raise exceptions on parsing failures
        
    Yields:
        MetadataChangeProposalWrapper instances containing lineage information
    """
    if procedure.code:
        datajob_input_output = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db=procedure.db,
            default_schema=procedure.schema,
            code=procedure.code,
            is_temp_table=is_temp_table,
            raise_=raise_,
        )

        if datajob_input_output:
            yield MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )
