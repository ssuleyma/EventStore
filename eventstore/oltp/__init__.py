"""
Important classes for the OLTP functionality in IBM Event Store:

    - :class:`eventstore.oltp.EventContext`
      Main entry point for the OLTP functionality.

Example:

>>> from eventstore.oltp import EventContext
>>> from eventstore.catalog import TableSchema
>>> from pyspark.sql.types import *
>>> with EventContext.create_database("testdb") as ctx:
...    schema = StructType([
...        StructField("userId", LongType(), nullable=True),
...        StructField("time", TimestampType(), nullable=True),
...        StructField("productId", IntegerType(), nullable=True),
...        StructField("rating", IntegerType(), nullable=True),
...        StructField("review", StringType(), nullable=True)
...    ])
...    tableSchema = TableSchema("reviews", schema,
...                              sharding_columns=["userId"],
...                              pk_columns=["userId", "time"])
...    ctx.create_table("reviews", tableSchema)
>>> EventContext.drop_database("testdb")
>>> EventContext.clean_up()
"""
from __future__ import absolute_import

from eventstore.oltp.context import EventContext
import eventstore.oltp.row_generator

__all__ = [
    "EventContext", "row_generator"
]
