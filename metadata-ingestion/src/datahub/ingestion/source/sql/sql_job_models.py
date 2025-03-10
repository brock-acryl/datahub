from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Union

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
)


@dataclass
class ProcedureDependency:
    """
    Represents a dependency of a stored procedure on another database object.
    """

    db: str
    schema: str
    name: str
    type: str
    env: str
    server: Optional[str] = None
    source: str = None  # Will be set by the specific source implementation

    @property
    def full_name(self) -> str:
        return f"{self.db}.{self.schema}.{self.name}"


@dataclass
class ProcedureLineageStream:
    """
    Represents lineage information for a stored procedure.
    """

    dependencies: List[ProcedureDependency]

    @property
    def as_property(self) -> Dict[str, str]:
        return {
            f"{dep.db}.{dep.schema}.{dep.name}": dep.type for dep in self.dependencies
        }


@dataclass
class SQLJob:
    """
    Generic representation of a SQL job (could be a stored procedure, job, etc.)
    """

    db: str
    platform_instance: Optional[str]
    name: str
    env: str
    source: str  # Will be set by the specific source implementation
    type: str = "JOB"

    @property
    def formatted_name(self) -> str:
        return self.name.replace(",", "-")

    @property
    def full_type(self) -> str:
        return f"({self.source},{self.formatted_name},{self.env})"

    @property
    def orchestrator(self) -> str:
        return self.source

    @property
    def cluster(self) -> str:
        return f"{self.env}"


@dataclass
class SQLProceduresContainer:
    """
    Container for a group of stored procedures.
    """

    db: str
    platform_instance: Optional[str]
    name: str
    env: str
    source: str  # Will be set by the specific source implementation
    type: str = "JOB"

    @property
    def formatted_name(self) -> str:
        return self.name.replace(",", "-")

    @property
    def orchestrator(self) -> str:
        return self.source

    @property
    def cluster(self) -> str:
        return f"{self.env}"

    @property
    def full_type(self) -> str:
        return f"({self.source},{self.name},{self.env})"


@dataclass
class ProcedureParameter:
    """
    Represents a parameter of a stored procedure.
    """

    name: str
    type: str
    direction: Optional[str] = None  # IN, OUT, INOUT, etc.
    default_value: Optional[str] = None

    @property
    def properties(self) -> Dict[str, str]:
        props = {"type": self.type}
        if self.direction:
            props["direction"] = self.direction
        if self.default_value:
            props["default_value"] = self.default_value
        return props


@dataclass
class StoredProcedure:
    """
    Generic representation of a stored procedure.
    """

    db: str
    schema: str
    name: str
    flow: Union[SQLJob, SQLProceduresContainer]
    type: str = "STORED_PROCEDURE"
    source: str = None  # Will be set by the specific source implementation
    code: Optional[str] = None
    language: Optional[str] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    comment: Optional[str] = None
    owner: Optional[str] = None
    parameters: Optional[List[ProcedureParameter]] = None
    return_type: Optional[str] = None

    @property
    def full_type(self) -> str:
        return self.source.upper() + "_" + self.type

    @property
    def formatted_name(self) -> str:
        return self.name.replace(",", "-")

    @property
    def full_name(self) -> str:
        return f"{self.db}.{self.schema}.{self.formatted_name}"

    @property
    def escape_full_name(self) -> str:
        return f"{self.db}.{self.schema}.{self.formatted_name}"


@dataclass
class JobStep:
    """
    Represents a step in a SQL job.
    """

    job_name: str
    step_name: str
    flow: SQLJob
    type: str = "JOB_STEP"
    source: str = None  # Will be set by the specific source implementation
    command: Optional[str] = None
    subsystem: Optional[str] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None

    @property
    def formatted_step(self) -> str:
        return self.step_name.replace(",", "-").replace(" ", "_").lower()

    @property
    def formatted_name(self) -> str:
        return self.job_name.replace(",", "-")

    @property
    def full_type(self) -> str:
        return self.source.upper() + "_" + self.type

    @property
    def full_name(self) -> str:
        return self.formatted_name


@dataclass
class SQLDataJob:
    """
    Represents a data job in DataHub, which could be a stored procedure or a job step.
    """

    entity: Union[StoredProcedure, JobStep]
    type: str = "dataJob"
    source: str = None  # Will be set by the specific source implementation
    external_url: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    incoming: List[str] = field(default_factory=list)
    outgoing: List[str] = field(default_factory=list)
    input_jobs: List[str] = field(default_factory=list)
    job_properties: Dict[str, str] = field(default_factory=dict)

    @property
    def urn(self) -> str:
        return make_data_job_urn(
            orchestrator=self.entity.flow.orchestrator,
            flow_id=self.entity.flow.formatted_name,
            job_id=self.entity.formatted_name,
            cluster=self.entity.flow.cluster,
            platform_instance=self.entity.flow.platform_instance,
        )

    def add_property(
        self,
        name: str,
        value: str,
    ) -> None:
        self.job_properties[name] = value

    @property
    def valued_properties(self) -> Dict[str, str]:
        if self.job_properties:
            return {k: v for k, v in self.job_properties.items() if v is not None}
        return self.job_properties

    @property
    def as_datajob_input_output_aspect(self) -> DataJobInputOutputClass:
        return DataJobInputOutputClass(
            inputDatasets=sorted(self.incoming),
            outputDatasets=sorted(self.outgoing),
            inputDatajobs=sorted(self.input_jobs),
        )

    @property
    def as_datajob_info_aspect(self) -> DataJobInfoClass:
        return DataJobInfoClass(
            name=self.entity.full_name,
            type=self.entity.full_type,
            description=self.description,
            customProperties=self.valued_properties,
            externalUrl=self.external_url,
            status=self.status,
        )

    @property
    def as_maybe_platform_instance_aspect(self) -> Optional[DataPlatformInstanceClass]:
        if self.entity.flow.platform_instance:
            return DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.entity.flow.orchestrator),
                instance=make_dataplatform_instance_urn(
                    platform=self.entity.flow.orchestrator,
                    instance=self.entity.flow.platform_instance,
                ),
            )
        return None

    @property
    def as_container_aspect(self) -> ContainerClass:
        key_args = dict(
            platform=self.entity.flow.orchestrator,
            instance=self.entity.flow.platform_instance,
            env=self.entity.flow.env,
            database=self.entity.flow.db,
        )
        container_key = (
            SchemaKey(
                schema=self.entity.schema,
                **key_args,
            )
            if isinstance(self.entity, StoredProcedure)
            else DatabaseKey(
                **key_args,
            )
        )
        return ContainerClass(container=container_key.as_urn())


@dataclass
class SQLDataFlow:
    """
    Represents a data flow in DataHub, which could be a job or a procedures container.
    """

    entity: Union[SQLJob, SQLProceduresContainer]
    type: str = "dataFlow"
    source: str = None  # Will be set by the specific source implementation
    external_url: Optional[str] = None
    flow_properties: Dict[str, str] = field(default_factory=dict)

    def add_property(
        self,
        name: str,
        value: str,
    ) -> None:
        self.flow_properties[name] = value

    @property
    def urn(self) -> str:
        return make_data_flow_urn(
            orchestrator=self.entity.orchestrator,
            flow_id=self.entity.formatted_name,
            cluster=self.entity.cluster,
            platform_instance=self.entity.platform_instance,
        )

    @property
    def as_dataflow_info_aspect(self) -> DataFlowInfoClass:
        return DataFlowInfoClass(
            name=self.entity.formatted_name,
            customProperties=self.flow_properties,
            externalUrl=self.external_url,
        )

    @property
    def as_maybe_platform_instance_aspect(self) -> Optional[DataPlatformInstanceClass]:
        if self.entity.platform_instance:
            return DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.entity.orchestrator),
                instance=make_dataplatform_instance_urn(
                    self.entity.orchestrator, self.entity.platform_instance
                ),
            )
        return None

    @property
    def as_container_aspect(self) -> ContainerClass:
        databaseKey = DatabaseKey(
            platform=self.entity.orchestrator,
            instance=self.entity.platform_instance,
            env=self.entity.env,
            database=self.entity.db,
        )
        return ContainerClass(container=databaseKey.as_urn())
