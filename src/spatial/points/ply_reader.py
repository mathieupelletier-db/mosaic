from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType
import os
from pyspark.sql import SparkSession

class FakeDataSource(DataSource):
    """
    A fake data source for PySpark to generate synthetic data using the `faker` library.
    Options:
    - numRows: specify number of rows to generate. Default value is 3.
    """

    @classmethod
    def name(cls):
        return "fake"

    def schema(self):
        return "name string, date string, zipcode string, state string"

    def reader(self, schema: StructType):
        return FakeDataSourceReader(schema, self.options)

class FakeDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        from faker import Faker
        fake = Faker()
        # Note: every value in this `self.options` dictionary is a string.
        num_rows = int(self.options.get("numRows", 3))
        for _ in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)

def register_fake_data_source():
    if os.getenv("IS_SERVERLESS") == "TRUE":
        raise RuntimeError(
            "Error: This data source can only be executed in a non-serverless context. "
            "Please attach the notebook to a traditional compute cluster and try again."
        )
    
    spark = SparkSession.getActiveSession()
    try:
        spark.dataSource.register(FakeDataSource)
        print("Custom data source 'fake' registered successfully.")
    except AttributeError:
        print("Error registering custom data source: PySpark custom data sources are not supported in this environment.")            