"""Configuration file for the house price prediction project."""

from typing import Any

import yaml
from pydantic import BaseModel


class ProjectConfig(BaseModel):
    """Configuration for the house price prediction project loaded from YAML.

    Handles feature specification, catalog, schema details and experiment parameters.
    Supports environment-specific configuration overrides.
    """

    num_features: list[str]
    cat_features: list[str]
    target: str
    catalog_name: str
    schema_name: str
    parameters: dict[str, Any]

    @classmethod
    def from_yaml(cls, config_path: str, env: str = "dev") -> "ProjectConfig":
        """Load configuration from a YAML file."""
        if env not in ["dev", "acc", "prod"]:
            raise ValueError("Invalid env: {env} Environment must be either 'dev', 'acc' or 'prod'")

        with open(config_path) as f:
            config_dict = yaml.safe_load(f)
            config_dict["catalog_name"] = config_dict[env]["catalog_name"]
            config_dict["schema_name"] = config_dict[env]["schema_name"]

            return cls(**config_dict)


class Tags(BaseModel):
    """Represents a set of tags for a Git commit.

    Contains information about the Git SHA, branch and job run ID.
    """

    git_sha: str
    branch: str
    job_run_id: str
