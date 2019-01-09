"""
Important classes for the OLAP/Spark functionality in IBM Event Store:

    - :class:`eventstore.sql.EventSession`
      Main entry point for the OLAP/Spark functionality.

"""

from __future__ import absolute_import

from eventstore.sql.session import EventSession

__all__ = [
    "EventSession"
]
