__all__ = [
    "FakeDataSourceReader",
    "FakeDataSource"
]

from .las_laz_reader import *

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
spark.dataSource.register(FakeDataSource)
