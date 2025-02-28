from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pydantic.fields import Field

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig


@dataclass
class BaseColumn:
    name: str
    ordinal_position: int
    is_nullable: bool
    data_type: str
    comment: Optional[str]


@dataclass
class BaseTable:
    name: str
    comment: Optional[str]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    size_in_bytes: Optional[int]
    rows_count: Optional[int]
    column_count: Optional[int] = None
    ddl: Optional[str] = None


@dataclass
class BaseView:
    name: str
    comment: Optional[str]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    view_definition: Optional[str]
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None
    column_count: Optional[int] = None


@dataclass
class BaseStoredProcedure:
    name: str
    schema: str
    language: Optional[str] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    definition: Optional[str] = None
    comment: Optional[str] = None
    owner: Optional[str] = None
    parameters: Optional[list] = None
    return_type: Optional[str] = None
    
    def to_stored_procedure(self, db: str, flow, source: str):
        """
        Convert to a StoredProcedure from sql_job_models.
        
        Args:
            db: The database name
            flow: An instance of SQLJob or SQLProceduresContainer
            source: The source platform name
            
        Returns:
            A StoredProcedure instance from sql_job_models
        """
        from datahub.ingestion.source.sql.sql_job_models import StoredProcedure, ProcedureParameter
        
        # Convert parameters if they exist
        converted_params = None
        if self.parameters:
            converted_params = [
                ProcedureParameter(
                    name=param.get("name", ""),
                    type=param.get("type", ""),
                    direction=param.get("direction"),
                    default_value=param.get("default_value")
                )
                for param in self.parameters
            ]
            
        return StoredProcedure(
            db=db,
            schema=self.schema,
            name=self.name,
            flow=flow,
            source=source,
            code=self.definition,
            language=self.language,
            created=self.created,
            last_altered=self.last_altered,
            comment=self.comment,
            owner=self.owner,
            parameters=converted_params,
            return_type=self.return_type
        )


class SQLAlchemyGenericConfig(SQLCommonConfig):
    platform: str = Field(
        description="Name of platform being ingested, used in constructing URNs."
    )
    connect_uri: str = Field(
        description="URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls"
    )

    def get_sql_alchemy_url(self):
        return self.connect_uri


@platform_name("SQLAlchemy", id="sqlalchemy")
@config_class(SQLAlchemyGenericConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class SQLAlchemyGenericSource(SQLAlchemySource):
    """
    The `sqlalchemy` source is useful if we don't have a pre-built source for your chosen
    database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/)
    defined elsewhere. In order to use this, you must `pip install` the required dialect packages yourself.

    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling.
    """

    def __init__(self, config: SQLAlchemyGenericConfig, ctx: PipelineContext):
        super().__init__(config, ctx, config.platform)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLAlchemyGenericConfig.parse_obj(config_dict)
        return cls(config, ctx)
