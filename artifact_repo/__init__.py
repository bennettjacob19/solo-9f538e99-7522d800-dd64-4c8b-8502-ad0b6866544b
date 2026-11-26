"""Local artifact repository: numbered-shard upload, range-friendly release.

Importing the package does not import Flask, so the storage engine and the
planning logic can be used (and tested) without web dependencies.
"""

from .store import Repo

__all__ = ["Repo", "create_app"]


def create_app(*args, **kwargs):
    from .web import create_app as _create_app

    return _create_app(*args, **kwargs)
