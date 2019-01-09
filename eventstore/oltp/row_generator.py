"""
A utility module that generates random rows.
"""
import string
from datetime import datetime, timedelta, tzinfo
from eventstore.catalog import TableSchema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, TimestampType


class UTC(tzinfo):
    """UTC time zone"""

    def utcoffset(self, dt):
        """
        Return the UTC offset to the timezone.
        :param dt: datetime
        :return: a timedelta
        """
        return timedelta(0)

    def tzname(self, dt):
        """
        Return the name of the timezone.

        :param dt: datetime
        :return:
        """
        return "UTC"

    def dst(self, dt):
        """
        Return the daylight saving time adjustment.

        :param dt: datetime
        :return:
        """
        return timedelta(0)


def generate_reviews(num_reviews, batch_size):
    """
    Generate batches of random rows for reviews table.

    Produces rows for the `reviews` table:

    >>> schema = StructType([
    ...    StructField("productId", LongType(), nullable=True),
    ...    StructField("rating", LongType(), nullable=True),
    ...    StructField("review", StringType(), nullable=True),
    ...    StructField("time", TimestampType(), nullable=True),
    ...    StructField("userId", LongType(), nullable=True)
    ... ])
    >>> table_schema = TableSchema("reviews", schema,
    ...                            sharding_columns=["userId"],
    ...                            pk_columns=["userId", "time"])

    :param num_reviews: total number of rows to generate.
    :param batch_size: size of all batches except possibly the last
    :return: a batch of rows, list of `dict`
    :rtype: a generator producing a list of `dict`
    """
    import random
    random.seed(123456)
    num_processed = 0
    last_time = datetime(2017, 6, 19, 18, 59, 33, 12345, tzinfo=UTC())
    while num_processed < num_reviews:
        this_batch_size = min(batch_size, num_reviews-num_processed)
        batch = []
        for _ in range(this_batch_size):
            userId = random.randint(1, 10000)
            productId = random.randint(1, 1000000)
            time = last_time + timedelta(microseconds=random.randint(1000,1000000))
            last_time = time
            rating = random.randint(1, 5)
            review = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
            # batch.append(Row(userId=userId, time=time, productId=productId, rating=rating, review=review))
            batch.append(dict(userId=userId, time=time, productId=productId, rating=rating, review=review))
        num_processed += this_batch_size
        yield batch

def generate_tele(num_rows, batch_size):
    """
    Generate batches of random rows for SES::tele1 table.

    Produces rows for the `tele1` table:

    >>> schema = StructType([
    ...  StructField("satelliteId", IntegerType(), nullable=True),
    ...  StructField("timeStamp", TimestampType(), nullable=True),
    ...  StructField("metricId", IntegerType(), nullable=True),
    ...  StructField("metricValue", LongType(), nullable=True)
    ... ])
    >>> table_schema = TableSchema("tele1", schema,
    ...  sharding_columns=["satelliteId"],
    ...  pk_columns=["satelliteId", "timeStamp"])
    :param num_rows: total number of rows to generate.
    :param batch_size: size of all batches except possibly the last
    :return: a batch of rows, list of `dict`
    :rtype: a generator producing a list of `dict`
    """
    import random
    random.seed(123456)
    num_processed = 0
    last_time = datetime(2017, 6, 19, 18, 59, 33, 12345, tzinfo=UTC())
    while num_processed < num_rows:
        this_batch_size = min(batch_size, num_rows-num_processed)
        batch = []
        for _ in range(this_batch_size):
            satelliteId = random.randint(1, 24)
            metricId = random.randint(1, 2000)
            time = last_time + timedelta(microseconds=random.randint(1000,1000000))
            last_time = time
            metricValue = random.randint(1, 10000000)

            batch.append(dict(satelliteId=satelliteId, timeStamp=time, metricId=metricId, metricValue=metricValue))
        num_processed += this_batch_size
        yield batch

def generate_array(num_rows, batch_size):
    """
    Generate batches of random rows for SES::tele1 table.

    Produces rows for the `tele1` table:

    >>> schema = StructType([
    ...  StructField("satelliteId", IntegerType(), nullable=True),
    ...  StructField("timeStamp", TimestampType(), nullable=True),
    ...  StructField("metricId", IntegerType(), nullable=True),
    ...  StructField("metricValue", LongType(), nullable=True)
    ... ])
    >>> table_schema = TableSchema("tele1", schema,
    ...  sharding_columns=["satelliteId"],
    ...  pk_columns=["satelliteId", "timeStamp"])
    :param num_rows: total number of rows to generate.
    :param batch_size: size of all batches except possibly the last
    :return: a batch of rows, list of `dict`
    :rtype: a generator producing a list of `dict`
    """
    import random
    random.seed(123456)
    num_processed = 0
    last_time = datetime(2017, 6, 19, 18, 59, 33, 12345, tzinfo=UTC())
    while num_processed < num_rows:
        this_batch_size = min(batch_size, num_rows-num_processed)
        batch = []
        for _ in range(this_batch_size):
            satelliteId = random.randint(1, 24)
            metricId = random.randint(1, 2000)
            time = last_time + timedelta(microseconds=random.randint(1000,1000000))
            last_time = time
            metricValue = random.randint(1, 10000000)
            listInt = [random.randint(1, 10000),random.randint(1, 10000), random.randint(1, 10000) ]
            listShort = [random.randint(1, 10000),random.randint(1, 10000), random.randint(1, 10000) ]
            listLong = [long(random.randint(1, 10000000)),long(random.randint(1, 10000000)), long(random.randint(1, 10000000)) ]
            listByte = [random.randint(1, 100),random.randint(1, 100), random.randint(1, 100) ]
            listFloat = [float(random.randint(1, 10000000)),float(random.randint(1, 10000000)), float(random.randint(1, 10000000)) ]
            listDouble = [1123.2345, 12344.2222, 123424.3333]
            listBool = [True, False, True, False]
            listString = ['test', 'python', 'string']
            listDate = [datetime.today(), datetime.today() ]
            listTimestamp = [time, time]

            batch.append(dict(satelliteId=satelliteId, timeStamp=time, metricId=metricId, metricValue=metricValue,
                              arrInt=listInt, arrLong=listLong, arrShort=listShort, arrFloat=listFloat,
                              arrDouble=listDouble, arrByte=listByte, arrString=listString,
                              arrDate= listDate, arrTimestamp=listTimestamp,arrBoolean=listBool))

        num_processed += this_batch_size
        yield batch

def generate_map(num_rows, batch_size):
    """
    Generate batches of random rows for SES::tele1 table.

    Produces rows for the `tele1` table:

    >>> schema = StructType([
    ...  StructField("satelliteId", IntegerType(), nullable=True),
    ...  StructField("timeStamp", TimestampType(), nullable=True),
    ...  StructField("metricId", IntegerType(), nullable=True),
    ...  StructField("metricValue", LongType(), nullable=True)
    ... ])
    >>> table_schema = TableSchema("tele1", schema,
    ...  sharding_columns=["satelliteId"],
    ...  pk_columns=["satelliteId", "timeStamp"])
    :param num_rows: total number of rows to generate.
    :param batch_size: size of all batches except possibly the last
    :return: a batch of rows, list of `dict`
    :rtype: a generator producing a list of `dict`
    """
    import random
    random.seed(123456)
    num_processed = 0
    last_time = datetime(2017, 6, 19, 18, 59, 33, 12345, tzinfo=UTC())
    while num_processed < num_rows:
        this_batch_size = min(batch_size, num_rows-num_processed)
        batch = []
        for _ in range(this_batch_size):
            satelliteId = random.randint(1, 24)
            metricId = random.randint(1, 2000)
            time = last_time + timedelta(microseconds=random.randint(1000,1000000))
            last_time = time
            metricValue = random.randint(1, 10000000)
            mapInt = {1: datetime.today(), 2: datetime.today() + timedelta(microseconds=random.randint(1000,1000000))}
            mapLong = {long(random.randint(1, 10000000)): "str1", long(random.randint(1, 10000000)): "str2"}
            mapShort = {random.randint(1, 10000) : 100, random.randint(1, 10000): 200, random.randint(1, 10000): 300 }
            mapFloat = {float(random.randint(1, 10000000)): True, float(random.randint(1, 10000000)): False}
            mapDouble = {1123.2345: time, 12344.2222: time + timedelta(microseconds=random.randint(1000,1000000))}
            mapString = {'test': 1, 'python': 2, 'string': 3}
            mapByte = {random.randint(1, 100): long(random.randint(1, 10000000)),random.randint(1, 100): long(random.randint(1, 10000000))}
            mapDate = {datetime.today():float(random.randint(1, 10000000)),
                       datetime.today() + timedelta(microseconds=random.randint(1000,1000000)): float(random.randint(1, 10000000)) }
            mapTimestamp = {time: 1123.2345, time + timedelta(microseconds=random.randint(1000,1000000)): 3456.34567 }
            mapBool = {True: 1, False: 2}

            batch.append(dict(satelliteId=satelliteId, timeStamp=time, metricId=metricId, metricValue=metricValue,
                              mapInt=mapInt, mapLong=mapLong, mapShort=mapShort, mapFloat=mapFloat,
                              mapDouble=mapDouble, mapString=mapString, mapByte=mapByte, mapDate=mapDate,
                              mapTimestamp=mapTimestamp, mapBoolean=mapBool
                              ))

        num_processed += this_batch_size
        yield batch