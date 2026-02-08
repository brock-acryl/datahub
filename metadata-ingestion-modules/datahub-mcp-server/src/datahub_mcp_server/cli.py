"""CLI entry point for DataHub MCP Server."""

import argparse
import asyncio
import logging
import sys

from datahub_mcp_server.config import Config, DataHubConfig, ServerConfig
from datahub_mcp_server.server import DataHubMCPServer


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="DataHub MCP Server - Model Context Protocol integration for DataHub")

    parser.add_argument(
        "--datahub-url",
        type=str,
        default="http://localhost:8080",
        help="DataHub GMS URL (default: http://localhost:8080)",
    )

    parser.add_argument(
        "--token",
        type=str,
        help="DataHub access token for authentication",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: 30)",
    )

    parser.add_argument(
        "--default-env",
        type=str,
        default="PROD",
        help="Default environment/fabric for dataset URNs (default: PROD)",
    )

    parser.add_argument(
        "--transport",
        type=str,
        default="stdio",
        choices=["stdio", "sse"],
        help="MCP transport type (default: stdio)",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


async def run_server(config: Config) -> None:
    """Run the MCP server."""
    server = DataHubMCPServer(config)

    try:
        if config.server.transport == "stdio":
            from mcp.server.stdio import stdio_server

            async with stdio_server() as (read_stream, write_stream):
                await server.server.run(
                    read_stream,
                    write_stream,
                    server.server.create_initialization_options(),
                )
        else:
            raise NotImplementedError(f"Transport {config.server.transport} not implemented")
    finally:
        await server.cleanup()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    config = Config(
        datahub=DataHubConfig(
            url=args.datahub_url,
            token=args.token,
            timeout=args.timeout,
            default_env=args.default_env,
        ),
        server=ServerConfig(
            transport=args.transport,
            log_level=args.log_level,
        ),
    )

    logger.info(f"Starting DataHub MCP Server (connecting to {config.datahub.url})")

    try:
        asyncio.run(run_server(config))
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception:
        logger.exception("Server error")
        sys.exit(1)


if __name__ == "__main__":
    main()
