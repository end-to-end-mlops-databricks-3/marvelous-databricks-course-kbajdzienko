"""Utility functions for the house price prediction project."""

import os


class utils:
    """Utility functions for the house price prediction project."""

    @staticmethod
    def get_project_root() -> str:
        """Get the root directory of the project."""
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    @staticmethod
    def is_databricks() -> bool:
        """Check if the code is running in a Databricks environment."""
        return "DATABRICKS_RUNTIME_VERSION" in os.environ
