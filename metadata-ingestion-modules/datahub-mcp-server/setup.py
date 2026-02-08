"""Setup script for datahub-mcp-server."""

import os
import sys
from typing import Dict, List, Set

from setuptools import find_namespace_packages, setup

is_py38_or_newer = sys.version_info >= (3, 8)

package_metadata: Dict = {}
with open("./src/datahub_mcp_server/__init__.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()
    return description


base_requirements = {
    "mcp>=1.0.0",
    "aiohttp>=3.9.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
}

dev_requirements = {
    "ruff",
    "mypy>=1.9.0",
    "types-requests",
    "types-PyYAML",
}

integration_test_requirements = {
    "pytest>=7.2.0",
    "pytest-asyncio>=0.21.0",
    "pytest-timeout",
    "pytest-cov",
    "deepdiff",
    "requests-mock",
}

entry_points = {
    "console_scripts": [
        "datahub-mcp-server = datahub_mcp_server.cli:main",
    ],
}

setup(
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahubproject.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache License 2.0",
    description="DataHub MCP Server - Model Context Protocol integration for DataHub",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    packages=find_namespace_packages("src"),
    package_dir={"": "src"},
    package_data={
        "datahub_mcp_server": ["py.typed"],
    },
    entry_points=entry_points,
    python_requires=">=3.9",
    install_requires=list(base_requirements),
    extras_require={
        "dev": list(dev_requirements | integration_test_requirements),
        "integration-tests": list(integration_test_requirements),
    },
    zip_safe=False,
)
