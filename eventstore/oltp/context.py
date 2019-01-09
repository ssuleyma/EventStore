"""
This module implements the OLTP Context of the IBM Event Store.
"""
import json

from eventstore.catalog import ResolvedTableSchema
from eventstore.java_gateway import start_gateway
import eventstore.common
import threading


class EventContext(object):
    """
    The EventContext is the OLTP interface to the IBM Event Store.

    Create a new event context by invoking the :func:`create_database` class method
    in order to create a new database::

        >>> context = EventContext.create_database("foo")

    or by calling the class method :func:`open_database` to open an already existing
    database::

        >>> context = EventContext.get_event_context("foo")
    """

    _init_lock = threading.RLock()  # Lock to serialize setup of class vars
    _gateway = None             # Py4J gateway object
    _jvm = None                 # Py4J JVM object
    _jesc = None                # event store companion object

    def __init__(self, jes):
        EventContext._ensure_initialized()

        self._jes = jes

    def create_table(self, table_schema, table_properties=None):
        """
        Creates a new table.

        :param table_schema: table schema (unresolved), an instance of :class:`eventstore.catalog.TableSchema`
        """
        if table_schema.partition_columns is None:
            jtable_schema = self._jvm.TableSchema.fromPython(table_schema.table_name,
                                                             table_schema.schema.json(),
                                                             table_schema.sharding_columns,
                                                             table_schema.pk_columns)
        else:
            jtable_schema = self._jvm.TableSchema.fromPython(table_schema.table_name,
                                                             table_schema.schema.json(),
                                                             table_schema.sharding_columns,
                                                             table_schema.pk_columns,
                                                             table_schema.partition_columns)
        tab_prop = table_properties if table_properties is not None else {}
        option_error = self._jes.createTable(jtable_schema, tab_prop)
        if option_error.isDefined():
            raise Exception(option_error.get())

    def create_table_with_index(self, table_schema, index_spec, table_properties=None):
        """
        Creates a new table with an index.

        :param table_schema: table schema (unresolved), an instance of :class:`eventstore.catalog.TableSchema`.
        :param index_spec: index specification, an instance of :class:`eventstore.catalog.IndexSpecification`
        """
        if table_schema.partition_columns is None:
           jtable_schema = self._jvm.TableSchema.fromPython(table_schema.table_name,
                                                         table_schema.schema.json(),
                                                         table_schema.sharding_columns,
                                                         table_schema.pk_columns)
        else:
           jtable_schema = self._jvm.TableSchema.fromPython(table_schema.table_name,
                                                         table_schema.schema.json(),
                                                         table_schema.sharding_columns,
                                                         table_schema.pk_columns,
                                                         table_schema.partition_columns)

        jindex_spec = self._jvm.IndexSpecification.fromPython(
            index_spec.json(), jtable_schema)
        tab_prop = table_properties if table_properties is not None else {}
        option_error = self._jes.createTableWithIndex(
            jindex_spec.tableSchema(), jindex_spec, tab_prop)
        if option_error.isDefined():
            raise Exception(option_error.get())

    def drop_table(self, table_name):
        """
        Removes an existing table.

        :param table_name: name of the table to remove
        """
        option_error = self._jes.dropTable(table_name)
        if option_error.isDefined():
            raise Exception(option_error.get())

    def open_database(self):
        """
        Explicitly opens the database.

        This method explicitly opens the database that is associated with this
        context in the IBM Event Store daemons.
        """
        option_error = self._jes.openDatabase()
        if option_error.isDefined():
            raise Exception(option_error.get())

    def get_table(self, table_name):
        """
        Retrieves the resolved schema for a table from the IBM Event Store
        daemons.

        :param table_name: name of the table to lookup
        :return: :class:`eventstore.catalog.ResolvedTableSchema` or
                 an exception if the specified table could not be found.
        """
        jresolved_table_schema = self._jes.getTable(table_name)
        return ResolvedTableSchema(jresolved_table_schema)

    def get_names_of_tables(self):
        """
        Return names of all tables in database.

        :return: names as a list of string
        """
        it = self._jes.getNamesOfTables()
        names = []
        while it.hasNext():
            names.append(it.next())
        return names

    def batch_insert(self, resolved_table_schema, rows):
        """
        Insert a batch of rows into the table.

        Example:

        >>> from eventstore.oltp.row_generator import generate_reviews
        >>> resolved_table_schema = ctx.get_table("reviews")
        >>> for row_batch in generate_reviews(num_reviews=20, batch_size=8):
        ...     ctx.batch_insert(resolved_table_schema, row_batch)

        :param resolved_table_schema: a :class:`eventstore.catalog.ResolvedTableSchema`
        :param rows: a list of `dict` with a column name string as key and column value as dict value.
        :return: nothing or an exception of an error occurs.
        """
        if( isinstance(resolved_table_schema, str) ):
            real_resolved_table_schema = self.get_table(resolved_table_schema)
            self.batch_insert(real_resolved_table_schema, rows)
        else: 
            schema = resolved_table_schema.schema
            internal = [schema.toInternal(row) for row in rows]
            list_of_dict = []
            for r in internal:
                d = dict()
                for f, v in zip(schema.fields, r):
                    d[f.name] = v
                list_of_dict.append(d)
            json_str = json.dumps(
                list_of_dict, separators=(',', ':'), sort_keys=True)
            option_error = self._jes.batchInsertFromPython(resolved_table_schema.jresolved_table_schema,
                                                           json_str)
            if option_error.isDefined():
                raise Exception(option_error.get())

    def stop(self):
        """Stops the EventContext. """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @classmethod
    def create_database(cls, dbname):
        """
        Create a new database and open context to it.

        :param dbname: name of the database to create.
        :return: :class:`eventstore.oltp.EventContext` or throws exception.
        """
        EventContext._ensure_initialized()
        jes = EventContext._jesc.createDatabase(dbname)
        return EventContext(jes)

    @classmethod
    def get_event_context(cls, dbname):
        """
        Open already existing database and return context to it.

        :param dbname: name of already existing database.
        :return: :class:`eventstore.oltp.EventContext` or throws exception
        """
        EventContext._ensure_initialized()
        jes = EventContext._jesc.getEventContext(dbname)
        return EventContext(jes)

    @classmethod
    def drop_database(cls, dbname):
        """
        Drop the specified database.

        :param dbname: name of the database to drop
        """
        EventContext._ensure_initialized()
        option_error = EventContext._jesc.dropDatabase(dbname)
        if option_error.isDefined():
            raise Exception(option_error.get())

    @classmethod
    def clean_up(cls):
        """
        Close all connections in the client connection pool.
        """
        if EventContext._gateway:
            EventContext._jesc.cleanUp()

    @classmethod
    def _ensure_initialized(cls):
        # Only get the lock if the gateway is not yet setup. This saves getting
        # the lock each time this is called.
        if not cls._gateway:
            with cls._init_lock:
                if not cls._gateway:
                    gateway = start_gateway()
                    cls._jvm = gateway.jvm

                    # Instantiate the ConfigurationReader first so we can update
                    # the Java's ConfirationReader before creating the
                    # EventContext.
                    eventstore.common.ConfigurationReader._jCR = \
                        cls._jvm.ConfigurationReader
                    eventstore.common.ConfigurationReader \
                        .updateScalaConfigReader()
                    cls._jesc = cls._jvm.com.ibm.event.oltp.EventContext

                    # Must be set last as this indicates everything is setup
                    cls._gateway = gateway
