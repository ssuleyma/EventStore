"""
This module implements the Spark/OLAP functionality of the IBM Event Store.
"""
from py4j.java_gateway import java_import
from pyspark.sql.session import SparkSession
from eventstore.sql.session import SparkSession


class ConfigurationReader(object):
    """
    """
    _jCR = None
    # python ConfigurationReader's cached setter values
    py_connectionEndpoints = None
    py_connectionTimeout = None
    py_connectionAckTimeout = None
    py_connectionRetry = None
    py_blusparkUser = None
    py_blusparkPassword = None
    py_blusparkSSLEnabled = None
    py_allowJoinPushDowns = None
    py_allowPrintSiQLQueriesIsSet = None
    py_loggerLevels = {} 

    # python ConfigurationReader's cached readonly values
    py_ensembleServerList = None
    py_zkEnsembleString = None


    #default values to be kept in-sync with Scala's configurationReader
    BLUSPARK_CONF = "bluspark.conf"
    BLUSPARK_USER = "admin"
    BLUSPARK_PASSWD = "admin"
    DefaultPrintSiQLQuery = False
    DefaultForClientConnectionRetry = 0
    DefaultForClientConnectionAckTimeout = 10
    DefaultForRollerFrequency = 600
    DefaultLoggerLevel = "WARN"
    DefaultForClientConnectionTimeout = 2

    # configuration parameters (keys entries in bluspark.conf)
    # ZK_CONN_STRING = "zookeeper.connectionString"
    # CLIENT_CONN_ENDPTS = "internal.client.connection.endpoints"
    # CLIENT_CONN_TIMEOUT = "internal.client.connection.timeout" # In minutes
    # CLIENT_CONN_ACK_TIMEOUT = "internal.client.connection.ackTimeout"
    # CLIENT_CONN_RETRY = "internal.client.connection.retry" # Int
    # HIVE_METASTORE_URIS = "internal.hive.metastore.uris"
    # HIVE_METASTORE_DB = "internal.hive.metastore.database"
    # ROLLER_FREQUENCY = "internal.roller.frequency" # In seconds
    # PRINT_SIQL_QUERY = "internal.client.print.siql"
    # BLUSPARK_LOGGER_LEVEL = "trace.defaultVerbosity"
    # CLIENT_ALLOW_JOINPD = "internal.client.allowJoinPushDowns"
    # SSL_ENABLED = "security.SSLEnabled"

    def __init__(self):
        """
        object constructor currently not used
        """
    # No setter for this
    @classmethod
    def ensembleServerList(cls):
        """
        ensembleServerList is a scala val and so, immutable
        :return: immutable list of connectionEndpoints
        """
        
        if cls._jCR :
            try:
                cls.py_ensembleServerList = cls._jCR.ensembleServerList()
            except Exception:
                return None
        return cls.py_ensembleServerList


    # No setter for this
    @classmethod
    def zkEnsembleString(cls):
        """
        """
        if cls._jCR :
            try:
                cls.py_zkEnsembleString = cls._jCR.zkEnsembleString()
            except Exception:
                return None
        return cls.py_zkEnsembleString

    @classmethod
    def setConnectionTimeout(cls, connTimeout):
        """
	* embleStringefault client timeout is 2 mins.
    	* For timeout < 2 min, default timeout 2 mins is set.
        """
        # cache the setting
        if connTimeout < cls.DefaultForClientConnectionTimeout:
           cls.py_connectionTimeout = cls.DefaultForClientConnectionTimeout
        else:
           cls.py_connectionTimeout = connTimeout

        if cls._jCR :
            cls._jCR.setConnectionTimeout( connTimeout )


    @classmethod
    def setConnectionAckTimeout(cls, connTimeout):
        """
        """
        # cache the setting
        cls.py_connectionAckTimeout = connTimeout
        if cls._jCR :
            cls._jCR.setConnectionAckTimeout( connTimeout )

    @classmethod
    def setConnectionRetry(cls, connRetry):
        """
        """
        # cache the setting
        cls.py_connectionRetry = connRetry
        if cls._jCR :
            cls._jCR.setConnectionRetry( connRetry )

    @classmethod
    def setConnectionEndpoints(cls, c_str):
        """
        """
        # update the local CR cache
        cls.py_connectionEndpoints = c_str
        if cls._jCR :
            # send the endpoint to CR
            cls._jCR.setConnectionEndpoints(c_str)

    @classmethod
    def getConnectionEndpoints(cls):
        """
        :return: connectionEndpoints
        """
        if cls._jCR :
            # if we have the gateway to scala CR, re-fetch the endpoint
            cls.py_connectionEndpoints = cls._jCR.getConnectionEndpoints()
        if  cls.py_connectionEndpoints :
            return  cls.py_connectionEndpoints
        return cls.py_connectionEndpoints

    @classmethod
    def setEventUser(cls, userString):
        """
        """
        # update the local CR cache
        cls.py_blusparkUser = userString
        if cls._jCR :
            # send the userString to CR
            cls._jCR.setEventUser(userString)

    @classmethod
    def setLoggerLevel(cls, logger_name, level):
        """
        """
        cls.py_loggerLevels[logger_name] = level
        if cls._jCR :
            cls._jCR.setLoggerLevel(logger_name, level)


    @classmethod
    def setEventPassword(cls, passwordString):
        """
        """
        # update the local CR cache
        cls.py_blusparkPassword = passwordString
        if cls._jCR :
            # send the passwordString to CR
            cls._jCR.setEventPassword(passwordString)

    @classmethod
    def setSSLEnabled(cls, SSLString):
        """
        """
        # update the local CR cache
        cls.py_blusparkSSLEnabled = SSLString.toLowerCase
        if cls._jCR :
            cls._jCR.setSSLEnabled(SSLString)

    @classmethod
    def setAllowJoinPushDowns(cls, allowJoinPD):
        """
        """
        # update the local CR cache
        cls.py_allowJoinPushDowns = allowJoinPD
        if cls._jCR :
            cls._jCR.setAllowJoinPushDowns(allowJoinPD)

    @classmethod
    def setAlloPrintSiQLQueries(cls, allowPrintSiQL):
        """
        """
        # update the local CR cache
        cls.py_allowPrintSiQLQueriesIsSet = True
        if cls._jCR :
            cls._jCR.setAlloPrintSiQLQueries(allowPrintSiQL)

    @classmethod
    def getEventUser(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_blusparkUser = cls._jCR.getEventUser()

        if cls.py_blusparkUser :
            return cls.py_blusparkUser
        return cls.BLUSPARK_USER

    """
    Getters with no setters, values set in bluspark.conf
    """

    @classmethod
    def hiveMetastoreURIS(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            return cls._jCR.hiveMetastoreURIS()
        return ""

    @classmethod
    def hiveMetastoreDB(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            return cls._jCR.hiveMetastoreDB()
        return ""

    @classmethod
    def blusparkLoggerLevel(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            return cls._jCR.blusparkLoggerLevel()
        return cls.DefaultLoggerLevel

    @classmethod
    def rollerFrequency(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            return cls._jCR.rollerFrequency()
        return cls.DefaultForRollerFrequency

    @classmethod
    def getConnectionTimeout(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_connectionTimeout = cls._jCR.getConnectionTimeout()

        if cls.py_connectionTimeout :
            return cls.py_connectionTimeout
        return cls.DefaultForClientConnectionTimeout


    @classmethod
    def getConnectionAckTimeout(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_connectionAckTimeout = cls._jCR.getConnectionAckTimeout()
        if cls.py_connectionAckTimeout :
            return cls.py_connectionAckTimeout
        return cls.DefaultForClientConnectionAckTimeout

    @classmethod
    def getConnectionRetry(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_connectionRetry = cls._jCR.getConnectionRetry()
        if cls.py_connectionRetry :
            return  cls.py_connectionRetry
        return cls.DefaultForClientConnectionRetry

    @classmethod
    def printSiQLQueryString(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_allowPrintSiQLQueriesIsSet = cls._jCR.printSiQLQueryString()
        if cls.py_allowPrintSiQLQueriesIsSet :
            return  cls.py_allowPrintSiQLQueriesIsSet
        return cls.DefaultPrintSiQLQuery

    @classmethod
    def SSLEnabled(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_blusparkSSLEnabled = cls._jCR.SSLEnabled()
        if cls.py_blusparkSSLEnabled:
            return  cls.py_blusparkSSLEnabled
        return True

    @classmethod
    def allowJoinPushDownsValuelientUser(cls):
        """
        """
        if cls._jCR :
            # we have the gateway to scala CR
            cls.py_allowJoinPushDowns = cls._jCR.allowJoinPushDownsValue()
        if cls.py_allowJoinPushDowns :
            return cls.py_allowJoinPushDowns
        return False

    @classmethod
    def updateScalaConfigReader(cls):
        """
        flush any Python CR settings to Scala's CR
        """
        if cls._jCR :
            # no setters for:
            # cls.py_ensembleServerList
            # cls.py_zkEnsembleString
            if cls.py_connectionEndpoints :
               cls._jCR.setConnectionEndpoints(cls.py_connectionEndpoints)
            if cls.py_connectionTimeout :
               cls._jCR.setConnectionTimeout(cls.py_connectionTimeout)
            if cls.py_connectionAckTimeout :
               cls._jCR.setConnectionAckTimeout( cls.py_connectionAckTimeout )
            if cls.py_connectionRetry :
               cls._jCR.setConnectionRetry( cls.py_connectionRetry )
            if cls.py_blusparkUser :
               cls._jCR.setEventUser(cls.py_blusparkUser)
            if cls.py_blusparkPassword :
               cls._jCR.setEventPassword(cls.py_blusparkPassword)
            if cls.py_blusparkSSLEnabled :
               cls._jCR.setSSLEnabled(cls.py_blusparkSSLEnabled)
            if cls.py_allowJoinPushDowns :
               cls._jCR.setAllowJoinPushDowns(cls.py_allowJoinPushDowns)
            if cls.py_allowPrintSiQLQueriesIsSet :
               cls._jCR.setAlloPrintSiQLQueries(cls.py_allowPrintSiQLQueriesIsSet)
            if cls.py_loggerLevels:
               for loggerName in cls.py_loggerLevels:
                   cls._jCR.setLoggerLevel(loggerName, cls.py_loggerLevels[loggerName])

