from pathlib import Path
from pyspark.sql import SparkSession
import laspy
import numpy as np
from typing import Generator, Optional, Tuple, Dict
import os

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import *
except ImportError as e:
    print(f"Error importing PySpark custom data sources: {e}. This feature is only available in Databricks Runtime 15.2 and above.")
    print("PySpark custom data sources are in Public Preview in Databricks Runtime 15.2 and above, and on serverless environment version 2. Streaming support is available in Databricks Runtime 15.3 and above.")
    raise

class LASToGeometryDataSourceReader(DataSourceReader):
    """
    Data source reader to read LAS/LAZ files and convert them to geometries.

    This class processes LAS/LAZ files to extract geometries (e.g., points, lines, or polygons)
    and associated metadata (tags). The output can be configured to include geometries in
    Well-Known Text (WKT), Well-Known Binary (WKB), or GeoJSON formats.

    Attributes:
        schema (StructType): The schema of the output data, including fields for ID, type, geometry, and tags.
        options (dict): Configuration options to customize the data reader.

    Options:
        - 1,2,3
        - iterator size (or not)
        - point format 0..9
        - `path` (str): The file path to the input PBF file. **Required**.
        - `geometryType` (str): The output geometry format. Supported values:
          - `"WKT"`: Well-Known Text (default).
          - `"WKB"`: Well-Known Binary.
          - `"GeoJSON"`: GeoJSON format.
        - `emptyTagFilter` (bool): Whether to exclude OSM elements with no tags. Default: `True`.
        - `keyFilter` (str): A key-based filter for OSM tags. Only elements with this key will be processed. Example: `"highway"`.
        - `tagFilter` (str): A filter based on specific key-value tag pairs. Should be a tuple-like string, e.g., `"('amenity', 'cafe')"`.
          Only elements with this key-value pair will be processed.

    Example Usage:
        df = (
            spark.read.format("las")
            .option("path", path)
            .option("geometryType", "WKT")
            .option("emptyTagFilter", True)
            .option("keyFilter", "building")
            .option("tagFilter", "('building', 'hospital')")
            .load()
        )
    """
    def __init__(self, schema: StructType, options: dict):
        """
        Initialize the PBFToGeometryDataSourceReader.

        Args:
            schema (StructType): The schema of the output data.
            options (dict): Options to configure the data reader, such as file path and filters.
        """
        self.schema: StructType = schema
        self.options: dict = options

    def read(self, partition: Optional[int] = None) -> Generator[Tuple[int, str, Optional[str], Dict[str, str]], None, None]:
        """
        Read the PBF file and yield geometry data for each OSM element.

        Args:
            partition (Optional[int]): Partition index, if applicable. Not implemented.

        Yields:
            tuple: A tuple containing the element ID, type, geometry, and tags.
        """
        # Extract options
        input_path: str = self.options.get("path")
        if not input_path:
            raise ValueError("The 'path' option is required.")

        # Handle file vs directory

        with laspy.open(input_path) as f:
            tags: Dict[str, str] = {}

            for points in f.chunk_iterator(10000):
                for point in points:
                    x_float = np.array(point.x).astype(float)
                    y_float = np.array(point.y).astype(float)
                    z_float = np.array(point.z).astype(float)
                    #print(x_float[0], y_float[0], z_float[0])

                    yield (x_float, y_float, z_float, point.intensity, tags)

        #yield (element.id, element.type_str(), geometry, tags)

class LASToGeometryDataSource(DataSource):
    """
    A custom data source to convert LAS/LAZ files to geometries in WKT, WKB, or GeoJSON format,
    including tags for each object, using MapType for tags.
    """
    @classmethod
    def name(cls) -> str:
        """
        Get the name of the data source.

        Returns:
            str: The name of the data source.
        """
        return "las"

    def schema(self) -> StructType:
        """
        Define the schema for the output data.

        Returns:
            StructType: The schema including fields for ID, type, geometry, and tags.
            ['X',
 'Y',
 'Z',
 'intensity',
 'return_number',
 'number_of_returns',
 'scan_direction_flag',
 'edge_of_flight_line',
 'classification',
 'synthetic',
 'key_point',
 'withheld',
 'scan_angle_rank',
 'user_data',
 'point_source_id',
 'gps_time']
point format 0
        """
        return StructType([
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("z", FloatType(), True),
            StructField("intensity", ShortType(), True),
            StructField("tags", MapType(StringType(), StringType()), True)
        ])

    def reader(self, schema: StructType) -> LASToGeometryDataSourceReader:
        """
        Create a data source reader for reading the PBF file.

        Args:
            schema (StructType): The schema of the output data.

        Returns:
            PBFToGeometryDataSourceReader: An instance of the data source reader.
        """
        return LASToGeometryDataSourceReader(schema, self.options)

def register_las_data_source():
    if os.getenv("IS_SERVERLESS") == "TRUE":
        raise RuntimeError(
            "Error: This data source can only be executed in a non-serverless context. "
            "Please attach the notebook to a traditional compute cluster and try again."
        )
    
    spark = SparkSession.getActiveSession()
    try:
        spark.dataSource.register(LASToGeometryDataSource)
        print("Custom data source 'las' registered successfully.")
    except AttributeError:
        print("Error registering custom data source: PySpark custom data sources are not supported in this environment.")
