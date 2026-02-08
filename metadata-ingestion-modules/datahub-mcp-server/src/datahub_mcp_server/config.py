"""Configuration for DataHub MCP Server."""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataHubConfig(BaseSettings):
    """DataHub connection configuration."""

    model_config = SettingsConfigDict(
        env_prefix="DATAHUB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    url: str = Field(
        default="http://localhost:8080",
        description="DataHub GMS URL",
    )

    token: Optional[str] = Field(
        default=None,
        description="DataHub access token for authentication",
    )

    timeout: int = Field(
        default=30,
        description="Request timeout in seconds",
    )

    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for failed requests",
    )

    default_env: str = Field(
        default="PROD",
        description="Default environment/fabric for dataset URNs",
    )


class ServerConfig(BaseSettings):
    """MCP Server configuration."""

    model_config = SettingsConfigDict(
        env_prefix="MCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    transport: str = Field(
        default="stdio",
        description="MCP transport type (stdio, sse)",
    )

    log_level: str = Field(
        default="INFO",
        description="Logging level",
    )

    enable_async_operations: bool = Field(
        default=False,
        description="Enable asynchronous operation tracking",
    )


class Config:
    """Combined configuration."""

    def __init__(
        self,
        datahub: Optional[DataHubConfig] = None,
        server: Optional[ServerConfig] = None,
    ) -> None:
        self.datahub = datahub or DataHubConfig()
        self.server = server or ServerConfig()

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            datahub=DataHubConfig(),
            server=ServerConfig(),
        )
