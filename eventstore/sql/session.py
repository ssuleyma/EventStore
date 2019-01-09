"""
This module implements the Spark/OLAP functionality of the IBM Event Store.
"""
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession, DataFrame
import eventstore.common


class EventSession(SparkSession):
    """
    The Spark SQL entry point to IBM Event Store API.

    :param spark_context: a Python :class:`pyspark.SparkContext`
    :param database_name: database name to which bind this session is bound

    In EventSession is created from a SparkContext, which,
    in turn can be obtained from a SparkSession. An EventSession is a
    SparkSession with the following additional methods:

    - .. py:method:: open_database()
    - .. py:function:: load_event_table(table_name)
    - .. py:function:: get_names_of_tables()

    An example:

    >>> from pyspark.sql import SparkSession
    >>> sparkSession = SparkSession.builder \\
    ...                            .appName("EventStore SQL in Python") \\
    ...                            .getOrCreate()
    >>> eventSession = EventSession(sparkSession.sparkContext, "TestDB")
    >>> eventSession.open_database()
    >>> eventSession.load_event_table("Reviews") \\
    ...             .createOrReplaceTempView("Reviews")
    >>> df = eventSession.sql("SELECT * FROM Reviews")
    """

    #: The Py4J gateway into the JVM
    _gateway = None

    def __init__(self, spark_context, database_name):
        EventSession._ensure_initialized(spark_context._gateway)

        # if there are any ConfigurationReader settings, push it down to scala
        # before the rest of the initializtion code uses the settings
        #ok:eventstore.common.ConfigurationReader._jCR = SparkSession._instantiatedContext._jvm.ConfigurationReader
        eventstore.common.ConfigurationReader._jCR = spark_context._jvm.ConfigurationReader
        eventstore.common.ConfigurationReader.updateScalaConfigReader()

        self._spark_context = spark_context
        self._database_name = database_name

        # spark_context is the Python SparkContext
        # spark_context._jsc is the JavaSparkContext
        # spark_context._jsc.sc() returns the actual Scala SparkContext
        self._jsc = spark_context._jsc.sc()
        self._jvm = spark_context._jvm
        self._jes = self._jvm.EventSession(self._jsc, database_name)
        super(EventSession, self).__init__(spark_context, self._jes)

    def open_database(self):
        """
        Open the database in this session.

        :return: nothing or an exception
        """
        self._jes.openDatabase()

    def get_names_of_tables(self):
        """
        Return names of all tables in database.

        :return: names as a list of string
        """
        return self._jes.listTables().collect()

    def load_event_table(self, table_name):
        """
        Load table from IBM EventStore into DataFrame.

        :param table_name: name of the table to load.
        :return: A :class:`pyspark.sql.DataFrame`
        """
        jdf = self._jes.loadEventTable(table_name)
        return DataFrame(jdf, self._wrapped)

    def get_table_names_and_schemas(self):
        """
        Return all table names from the database and their
        corresponding resolved table schema.

        :return: A list of table name strings and :class:`pyspark.catalog.ResolvedTableSchema`
                 tuples.
        """
        jarray = self._jes.listTablesAndSchemas()
        pairs = []
        for t in jarray:
            from eventstore.catalog import ResolvedTableSchema
            pairs.append((t._1(), ResolvedTableSchema(t._2())))
        return pairs

    def set_sql_config_string(self, key, value):
        self._jes.setSQLConfString(key, value)

    def get_sql_config_string(self, key):
        return self._jes.getSQLConfString(key)

    def set_query_read_option(self, opt):
        """
        Set a query read option to be used by subsequent queries in this session.

        A query read option indicates the level of read consistency required.  A
        strict consistency like SnapshotNow will get the most
        consistent data but may have to wait for the latest data to be made
        available.  The other levels are not as strict so would not incur the
        wait but might not have the most recently inserted data
        (SnapshotAny) or could contain duplicate rows (SnapshotNone).

        :param opt: Read option to use in the session
        :type opt: SnapshotNow, SnapshotAny, or SnapshotNone
        """
        self._jes.setQueryReadOption(opt)

    def get_query_read_option(self):
        """
        Return the session's current read option

        :return String representation of the current query read option
        """
        return self._jes.getQueryReadOptionAsString()

    @property
    def parallel_degree(self):
        """
        Property: Get or set the degree of parallelism in the IBM Event Store engine(s).
        That is the number of parallel threads that execute a given
        query inside one Event Store engine.
        """
        return self._jes.getParallelDegree()

    @parallel_degree.setter
    def parallel_degree(self, degree):
        self._jes.setParallelDegree(degree)

    def enable_index_based_plan(self):
        """
        Allow creation of index based plans in this session.
        """
        self._jes.enableIndexBasedPlan()

    def disable_index_based_plan(self):
        """
        Disable creation of index based plans in this session.  All plans will
        be run as table scans.
        """
        self._jes.disableIndexBasedPlan()

    def is_index_base_plan_enabled(self):
        """
        Return state indicating whether index based plans are enabled for this
        session.
        :return: True if index based plans are enabled.  False otherwise.
        """
        return self._jes.isIndexBasedPlanEnabled()

    @classmethod
    def _ensure_initialized(cls, gateway):
        if not cls._gateway:
            java_import(gateway.jvm, "com.ibm.event.catalog.*")
            java_import(gateway.jvm, "com.ibm.event.sql.*")
            java_import(gateway.jvm, "com.ibm.event.common.*")
            java_import(gateway.jvm, "org.apache.spark.sql.ibm.event.*")

            cls._gateway = gateway
