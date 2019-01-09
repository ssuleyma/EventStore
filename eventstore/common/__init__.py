"""
Important classes for the OLAP/Spark functionality in IBM Event Store:

    - :class:`eventstore.common.ConfigurationReader`
      Main entry point for the OLAP/Spark functionality.

"""

from __future__ import absolute_import

from eventstore.common.configuration_reader import ConfigurationReader

__all__ = [
    "ConfigurationReader"
]
