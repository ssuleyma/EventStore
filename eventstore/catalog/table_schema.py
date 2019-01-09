"""
Representation of IBM Event Store table schemas.
"""
import json


class TableSchema(object):
    """
    The table schema describes the relational structure of IBM Event
    Store tables. These instances can be used to create tables, however,
    the lack information that is provided by the IBM Event Store daemons
    that is needed to use them for other operations. For those, use
    :class:`eventstore.catalog.ResolvedTableSchema` instead.
    """

    def __init__(self, table_name, schema, sharding_columns, pk_columns, partition_columns = None ):
        """
        Creates a table schema.

        :param table_name: name of the table
        :param schema: underlying Spark schema
        :param sharding_columns: list of column names that comprise the sharding key
        :param pk_columns: list of column names that comprise the primary key
        """
        self.table_name = table_name
        self.schema = schema
        self.sharding_columns = sharding_columns
        self.pk_columns = pk_columns
        self.partition_columns = partition_columns

    def __str__(self):
        return "TableSchema(tableName={}, schema={}, sharding_columns={}, pk_columns={}, partition_columns={})".format(
            self.table_name, self.schema, self.sharding_columns, self.pk_columns, self.partition_columns)

    @classmethod
    def from_json(cls, json):
        return TableSchema(json["tableName"], json["schema"],
                           json["shardingColumns"], json["pkColumns"])

    def json_value(self):
        """
         Return a value that is used by `json` to create a JSON representation of this object.

         :return: a dict value
         """
        return {"tableName": self.table_name,
                "schema": self.schema.jsonValue(),
                "shardingColumns": self.sharding_columns,
                "pkColumns": self.pk_columns,
                "partitionColumns": self.partition_columns}


    def json(self):
        """
        Create a JSON representation of the table schema.

        :returns: a JSON String.
        """
        return json.dumps(self.json_value(),
                          separators=(',', ':'), sort_keys=True)


class ResolvedTableSchema(TableSchema):
    """
    The resolved table scheme describes the relational structure of IBM Event
    Store tables. Resolved table schemas contain additional annotations created
    by the Event Store daemons and can be used to query tables in
    :class:`eventstore.oltp.EventContext` and for row inserts in
    :class:`eventstore.sql.EventSession`.
    """

    def __init__(self, jresolved_table_schema):
        """
        Create a resolved table schema from the underlying Java object.

        :param jresolved_table_schema: Java object of ResolvedTableSchema
        """
        from pyspark.sql.types import StructType
        table_name = jresolved_table_schema.tableName()
        json_schema = json.loads(jresolved_table_schema.schema().json())
        jschema = StructType.fromJson(json_schema)

        pk_columns = []
        it = jresolved_table_schema.pkColumns().iterator()
        while it.hasNext():
            pk_columns.append(it.next())

        sharding_columns = []
        it = jresolved_table_schema.shardingColumns().iterator()
        while it.hasNext():
            sharding_columns.append(it.next())

        self.pk_indexes = []
        it = jresolved_table_schema.pkIndex().iterator()
        while it.hasNext():
            jindex_spec = it.next()
            json_ispec = json.loads(jindex_spec.toJsonStr())
            self.pk_indexes.append(IndexSpecification.from_json(json_ispec))

        partition_columns = []
        it_option_wrapper = jresolved_table_schema.partitionColumns().iterator()
        if "{}".format(it_option_wrapper) == "non-empty iterator":
            #remove the Option wrapper
            it_partCols = it_option_wrapper.next().iterator()
            if "{}".format(it_partCols) == "non-empty iterator":
                while it_partCols.hasNext():
                    partition_columns.append(it_partCols.next())
            # else: an empty list for partition_columns
        elif "{}".format(it_option_wrapper) != "empty iterator":
            raise Exception("Expected returned value for partition_columns to be a JavaObject representing an iterator not {}".format(it))
        #else: print ("a wrapper for None")

        if len(partition_columns) > 0:
            super(ResolvedTableSchema, self).__init__(table_name, jschema,
                                                  sharding_columns, pk_columns, partition_columns)
        else:
            super(ResolvedTableSchema, self).__init__(table_name, jschema,
                                                  sharding_columns, pk_columns)
        self.jresolved_table_schema = jresolved_table_schema

    def __str__(self):
        return "ResolvedTableSchema(tableName={}, schema={}, sharding_columns={}, pk_columns={}, partition_columns={})".format(
                self.table_name, self.schema, self.sharding_columns, self.pk_columns, self.partition_columns)

class IndexSpecification(object):
    """
    The index specification defines an index with one or more `equality`, `sort`, and/or
    `include` columns.
    """

    def __init__(self, index_name, table_schema, equal_columns, sort_columns=None,
                 include_columns=None, index_id=None):
        """
        Creates a new index specification.

        :param index_name: String with name of index
        :param table_schema: Underlying table schema as an instance of :class:`eventstore.catalog.TableSchema`
        :param equal_columns: list of column name strings that will support equality predicates in the index.
        :param sort_columns: list of column name string that willo support range predicates in the index.
        :param include_columns: list of column names that are included in the index as payloads.
        :param index_id:
        """
        self.index_name = index_name
        self._table_schema = table_schema
        self.equal_columns = equal_columns
        self.sort_columns = sort_columns if sort_columns is not None else []
        self.include_columns = include_columns if include_columns is not None else []
        self.index_id = index_id

    @classmethod
    def from_json(cls, json_rep):
        sort_specs = []
        for sort_json in json_rep["sortColumns"]:
            sort_spec = SortSpecification.from_json(json.loads(sort_json))
            sort_specs.append(sort_spec)
        return IndexSpecification(json_rep["indexName"], None,
                                  json_rep["equalColumns"],
                                  sort_columns=sort_specs,
                                  include_columns=json_rep["includeColumns"],
                                  index_id=json_rep["indexId"])

    @property
    def table_schema(self):
        """
        Return the table schema.

        :return: :class:`eventstore.catalog.TableSchema`
        """
        return self._table_schema

    @table_schema.setter
    def table_schema(self, table_schema):
        self._table_schema = table_schema

    def json(self):
        """
        Produce a compact JSON representation of the index specification.

        :return: a JSON string
        """
        a_dict = {"indexName": self.index_name,
                  "tableSchema": self.table_schema.json_value(),
                  "equalColumns": self.equal_columns,
                  "sortColumns": [c.json_value() for c in self.sort_columns],
                  "includeColumns": self.include_columns,
                  "indexId": self.index_id}
        return json.dumps(a_dict, separators=(',', ':'), sort_keys=True)

    def __str__(self):
        return "IndexSpecification(indexName={}, indexID={}, equalColumns=({}), sortColumns=({}), includeColumns=({}))".format(
            self.index_name, self.index_id,
            ",".join(map(str, self.equal_columns)),
            ",".join(map(str, self.sort_columns)),
            ",".join(map(str, self.include_columns)))


class SortSpecification(object):
    """
    The sort specification determines the sort order of the name column.
    The order is determined by the :class:`eventstore.catalog.ColumnOrder`.
    """

    def __init__(self, column_name, order):
        """
        Create a new sort specification for the named column.

        :param column_name: name of the column
        :param order: sort order as a instance of :class:`eventstore.catalog.ColumnOrder`
        """
        self.column_name = column_name
        self.sort_order = order

    @classmethod
    def from_json(cls, json):
        """Construct a SortSpecication from a json object

        :param json: Must be a json object.  Not a string of a json, an actual
                     json dict as return from json package.
        :return: SortSpecification built based on json output
        """
        colOrder = ColumnOrder.get(json["sortOrder"])
        return SortSpecification(json["columnName"], colOrder)

    def __str__(self):
        return "SortSpecification(columnName={}, order={})".format(self.column_name, self.sort_order)

    def json_value(self):
        """
        Return a value that is used by `json` to create a JSON representation of this object.

        :return: a dict value
        """
        return {"columnName": self.column_name,
                "sortOrder": str(self.sort_order)}


class ColumnOrder(object):
    """
    The column order determines how column value are ordered (sorted).

    :cvar ColumnOrder.ASCENDING_NULLS_LAST: Ascending order with `null` greater than any value.
    :cvar ColumnOrder.DESCENDING_NULLS_LAST: Descending order with `null` greater than any value.
    :cvar ColumnOrder.ASCENDING_NULLS_FIRST: Ascending order with `null` smaller than any value.
    :cvar ColumnOrder.DESCENDING_NULLS_FIRST: Descending order with `null` smaller than any value.
    """

    _enums = dict()
    """Dictionary containing all enumeration strings with their ColumnOrder values."""

    def __init__(self, label):
        if label in ColumnOrder._enums:
            raise Exception("enum {} already defined".format(label))
        self.enum = len(ColumnOrder._enums)
        self.label = label
        ColumnOrder._enums[label] = self

    def __str__(self):
        return self.label

    @classmethod
    def get(cls, label):
        """
        Resolve ColumnOrder from label string.

        :param label: string of column order
        :return: instance of :class:`eventstore.catalog.ColumnOrder`
        """
        if label not in ColumnOrder._enums:
            raise Exception("{} is not a valid column order")
        return ColumnOrder._enums[label]

ColumnOrder.ASCENDING_NULLS_LAST = ColumnOrder("ascendingNullsLast")
ColumnOrder.DESCENDING_NULLS_LAST = ColumnOrder("descendingNullsLast")
ColumnOrder.ASCENDING_NULLS_FIRST = ColumnOrder("ascendingNullsFirst")
ColumnOrder.DESCENDING_NULLS_FIRST = ColumnOrder("descendingNullsFirst")
