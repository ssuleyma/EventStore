"""
Important classes for the catalog in IBM Event Store:

    - :class:`eventstore.catalog.TableSchema`
      Unresolved Table Schema.
      This type of table schema is used to create new tables.

    - :class:`eventstore.catalog.ResolvedTableSchema`
      Resolved Table Schema.
      A resolved table schema is returned by the IBM Event Store engine.
"""

from __future__ import absolute_import
from eventstore.catalog.table_schema import TableSchema, ResolvedTableSchema
from eventstore.catalog.table_schema import IndexSpecification, SortSpecification, ColumnOrder


__all__ = [
    "TableSchema", "ResolvedTableSchema", "IndexSpecification",
    "SortSpecification", "ColumnOrder"
]
