"""Python runtime library for devman-gen generated bridges.

Generated client packages use ManagerClient; generated server packages use
serve_manager / ManagerCore. The generator itself (devman-gen) is language
agnostic and is not required at runtime.
"""

from .client import ManagerClient, ManagerError
from .db import OwnershipDB
from .server import ManagerCore, RuntimeFunctionSpec, TripWatchdog, serve_manager

__all__ = [
    "ManagerClient",
    "ManagerError",
    "ManagerCore",
    "OwnershipDB",
    "RuntimeFunctionSpec",
    "TripWatchdog",
    "serve_manager",
]
