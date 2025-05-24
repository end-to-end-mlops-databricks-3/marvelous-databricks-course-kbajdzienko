"""House price package."""

# Imports
import importlib.metadata
import importlib.resources
from pathlib import Path

# Constants
THIS_DIR = Path(__file__).parent
PROJECT_DIR = (THIS_DIR / "../..").resolve()


def get_version() -> str:
    """Retrieve the version of the package.

    This function attempts to read the version from the installed package,
    and if not found, reads it from a version.txt file.

    Version attribute usage
    import house_price
    print(house_price.__version__)

    :return: The version string of the package.
    """
    package_name = __package__ or "house_price"

    try:
        # Try to read from the installed package
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        # If not installed, read from the version.txt file
        with importlib.resources.files(package_name).joinpath("../../version.txt").open("r", encoding="utf-8") as file:
            return file.read().strip()


__version__ = get_version()
