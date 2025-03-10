"""
Example demonstrating how to use the SQL job models for stored procedures.
This is not meant to be used directly but serves as a reference for implementing
stored procedure support in other SQL sources.
"""

from typing import Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_generic import BaseStoredProcedure
from datahub.ingestion.source.sql.sql_job_models import (
    SQLDataJob,
)
from datahub.ingestion.source.sql.sql_stored_procedure_helper import (
    add_lineage_to_data_job,
    convert_base_stored_procedure,
    create_data_flow,
    create_data_job,
    create_procedure_container,
    create_procedure_dependency,
)


def generate_stored_procedure_workunits(
    procedures: List[BaseStoredProcedure],
    db_name: str,
    platform_instance: Optional[str],
    env: str,
    source_platform: str,
    container_name: str = "stored_procedures",
) -> List[MetadataWorkUnit]:
    """
    Generate DataHub workunits for stored procedures.

    Args:
        procedures: List of BaseStoredProcedure instances
        db_name: Database name
        platform_instance: Optional platform instance name
        env: Environment name
        source_platform: Source platform name (e.g., "postgres", "mysql")
        container_name: Name for the container (default: "stored_procedures")

    Returns:
        List of MetadataWorkUnit instances
    """
    workunits = []

    # Create a container for all stored procedures
    container = create_procedure_container(
        db_name=db_name,
        platform_instance=platform_instance,
        container_name=container_name,
        env=env,
        source=source_platform,
    )

    # Create a data flow for the container
    data_flow = create_data_flow(
        container=container,
        source=source_platform,
        properties={"database": db_name},
    )

    # Add the data flow to workunits
    mcp = MetadataChangeProposalWrapper(
        entityType="dataFlow",
        entityUrn=data_flow.urn,
        aspectName="dataFlowInfo",
        aspect=data_flow.as_dataflow_info_aspect,
    )
    wu = MetadataWorkUnit(id=f"{data_flow.urn}-dataFlowInfo", mcp=mcp)
    workunits.append(wu)

    # Add platform instance if available
    if data_flow.as_maybe_platform_instance_aspect:
        mcp = MetadataChangeProposalWrapper(
            entityType="dataFlow",
            entityUrn=data_flow.urn,
            aspectName="dataPlatformInstance",
            aspect=data_flow.as_maybe_platform_instance_aspect,
        )
        wu = MetadataWorkUnit(id=f"{data_flow.urn}-dataPlatformInstance", mcp=mcp)
        workunits.append(wu)

    # Add container aspect
    mcp = MetadataChangeProposalWrapper(
        entityType="dataFlow",
        entityUrn=data_flow.urn,
        aspectName="container",
        aspect=data_flow.as_container_aspect,
    )
    wu = MetadataWorkUnit(id=f"{data_flow.urn}-container", mcp=mcp)
    workunits.append(wu)

    # Process each stored procedure
    for base_proc in procedures:
        # Convert to StoredProcedure
        stored_proc = convert_base_stored_procedure(
            base_proc=base_proc,
            db_name=db_name,
            container=container,
            source=source_platform,
        )

        # Create a data job for the stored procedure
        data_job = create_data_job(
            stored_proc=stored_proc,
            source=source_platform,
            description=base_proc.comment,
            properties={
                "language": base_proc.language or "",
                "return_type": base_proc.return_type or "",
                "owner": base_proc.owner or "",
            },
        )

        # Add the data job info to workunits
        mcp = MetadataChangeProposalWrapper(
            entityType="dataJob",
            entityUrn=data_job.urn,
            aspectName="dataJobInfo",
            aspect=data_job.as_datajob_info_aspect,
        )
        wu = MetadataWorkUnit(id=f"{data_job.urn}-dataJobInfo", mcp=mcp)
        workunits.append(wu)

        # Add platform instance if available
        if data_job.as_maybe_platform_instance_aspect:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataJob",
                entityUrn=data_job.urn,
                aspectName="dataPlatformInstance",
                aspect=data_job.as_maybe_platform_instance_aspect,
            )
            wu = MetadataWorkUnit(id=f"{data_job.urn}-dataPlatformInstance", mcp=mcp)
            workunits.append(wu)

        # Add container aspect
        mcp = MetadataChangeProposalWrapper(
            entityType="dataJob",
            entityUrn=data_job.urn,
            aspectName="container",
            aspect=data_job.as_container_aspect,
        )
        wu = MetadataWorkUnit(id=f"{data_job.urn}-container", mcp=mcp)
        workunits.append(wu)

        # Add empty input/output aspect (to be filled in by lineage analysis)
        mcp = MetadataChangeProposalWrapper(
            entityType="dataJob",
            entityUrn=data_job.urn,
            aspectName="dataJobInputOutput",
            aspect=data_job.as_datajob_input_output_aspect,
        )
        wu = MetadataWorkUnit(id=f"{data_job.urn}-dataJobInputOutput", mcp=mcp)
        workunits.append(wu)

    return workunits


def add_lineage_to_stored_procedure(
    data_job: SQLDataJob,
    input_tables: List[Dict[str, str]] = None,
    output_tables: List[Dict[str, str]] = None,
    env: str = "PROD",
    source_platform: str = "postgres",
) -> None:
    """
    Add lineage information to a stored procedure data job.

    Args:
        data_job: The SQLDataJob instance
        input_tables: List of input tables with db, schema, and name keys
        output_tables: List of output tables with db, schema, and name keys
        env: Environment name
        source_platform: Source platform name
    """
    # Process input tables
    if input_tables:
        input_dependencies = []
        for table in input_tables:
            dep = create_procedure_dependency(
                db=table["db"],
                schema=table["schema"],
                name=table["name"],
                dependency_type="TABLE",
                env=env,
                source=source_platform,
            )
            input_dependencies.append(dep)

        # Create lineage stream (commented out to pass lint)
        # lineage_stream = create_procedure_lineage_stream(dependencies=input_dependencies)

        # Add input datasets to data job
        from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance

        input_dataset_urns = []
        for dep in input_dependencies:
            dataset_urn = make_dataset_urn_with_platform_instance(
                platform=source_platform,
                name=f"{dep.db}.{dep.schema}.{dep.name}",
                platform_instance=None,
                env=env,
            )
            input_dataset_urns.append(dataset_urn)

        # Add to data job
        add_lineage_to_data_job(
            data_job=data_job,
            input_datasets=input_dataset_urns,
        )

    # Process output tables
    if output_tables:
        output_dependencies = []
        for table in output_tables:
            dep = create_procedure_dependency(
                db=table["db"],
                schema=table["schema"],
                name=table["name"],
                dependency_type="TABLE",
                env=env,
                source=source_platform,
            )
            output_dependencies.append(dep)

        # Create lineage stream (commented out to pass lint)
        # lineage_stream = create_procedure_lineage_stream(dependencies=output_dependencies)

        # Add output datasets to data job
        from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance

        output_dataset_urns = []
        for dep in output_dependencies:
            dataset_urn = make_dataset_urn_with_platform_instance(
                platform=source_platform,
                name=f"{dep.db}.{dep.schema}.{dep.name}",
                platform_instance=None,
                env=env,
            )
            output_dataset_urns.append(dataset_urn)

        # Add to data job
        add_lineage_to_data_job(
            data_job=data_job,
            output_datasets=output_dataset_urns,
        )


# Example usage:
def example_usage():
    """Example of how to use the stored procedure models in a source."""
    # Create some sample stored procedures
    sample_procedures = [
        BaseStoredProcedure(
            name="calculate_metrics",
            schema="analytics",
            language="SQL",
            definition="CREATE PROCEDURE calculate_metrics() BEGIN SELECT * FROM metrics; END;",
            comment="Calculates daily metrics",
            parameters=[
                {"name": "date", "type": "DATE", "direction": "IN"},
                {"name": "result", "type": "INT", "direction": "OUT"},
            ],
            return_type="void",
        ),
        BaseStoredProcedure(
            name="update_inventory",
            schema="inventory",
            language="SQL",
            definition="CREATE PROCEDURE update_inventory() BEGIN UPDATE products SET stock = stock - 1; END;",
            comment="Updates inventory levels",
            parameters=[
                {"name": "product_id", "type": "INT", "direction": "IN"},
                {"name": "quantity", "type": "INT", "direction": "IN"},
            ],
            return_type="void",
        ),
    ]

    # Generate workunits
    workunits = generate_stored_procedure_workunits(
        procedures=sample_procedures,
        db_name="my_database",
        platform_instance=None,
        env="PROD",
        source_platform="postgres",
    )

    # Print the number of workunits generated
    print(f"Generated {len(workunits)} workunits")

    # Example of adding lineage
    # First, we need to get the data job we created
    # In a real implementation, you would track this during workunit generation
    container = create_procedure_container(
        db_name="my_database",
        platform_instance=None,
        container_name="stored_procedures",
        env="PROD",
        source="postgres",
    )

    stored_proc = convert_base_stored_procedure(
        base_proc=sample_procedures[0],
        db_name="my_database",
        container=container,
        source="postgres",
    )

    data_job = create_data_job(
        stored_proc=stored_proc,
        source="postgres",
        description=sample_procedures[0].comment,
    )

    # Add lineage
    add_lineage_to_stored_procedure(
        data_job=data_job,
        input_tables=[
            {"db": "my_database", "schema": "public", "name": "metrics"},
            {"db": "my_database", "schema": "public", "name": "users"},
        ],
        output_tables=[
            {"db": "my_database", "schema": "analytics", "name": "daily_metrics"},
        ],
        env="PROD",
        source_platform="postgres",
    )

    # In a real implementation, you would then create a new workunit with the updated
    # dataJobInputOutput aspect and emit it


if __name__ == "__main__":
    example_usage()
