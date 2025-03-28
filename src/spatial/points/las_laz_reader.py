from pathlib import Path
from pyspark.sql import SparkSession
import osmium
import ast
from typing import Generator, Optional, Tuple, Dict
import os

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
except ImportError as e:
    print(f"Error importing PySpark custom data sources: {e}. This feature is only available in Databricks Runtime 15.2 and above.")
    print("PySpark custom data sources are in Public Preview in Databricks Runtime 15.2 and above, and on serverless environment version 2. Streaming support is available in Databricks Runtime 15.3 and above.")
    raise

class LasToArrayDataSourceReader(DataSourceReader):
    """
    Data source reader to read OSM PBF files and convert them to geometries.

    This class processes OSM PBF files to extract geometries (e.g., points, lines, or polygons)
    and associated metadata (tags). The output can be configured to include geometries in
    Well-Known Text (WKT), Well-Known Binary (WKB), or GeoJSON formats.

    Attributes:
        schema (StructType): The schema of the output data, including fields for ID, type, geometry, and tags.
        options (dict): Configuration options to customize the data reader.

    Options:
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
            spark.read.format("pbf")
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

        geometry_type: str = self.options.get("geometryType", "WKT").upper()
        if geometry_type not in ["WKT", "WKB", "GEOJSON"]:
            raise ValueError("Invalid geometryType option. Choose 'WKT', 'WKB', or 'GeoJSON'.")

        # Ensure the file exists
        input_file: Path = Path(input_path)
        if not input_file.exists():
            raise FileNotFoundError(f"Input file {input_path} not found.")

        # Choose the appropriate geometry factory
        if geometry_type == "WKT":
            geometry_factory = osmium.geom.WKTFactory()
        elif geometry_type == "WKB":
            geometry_factory = osmium.geom.WKBFactory()
        elif geometry_type == "GEOJSON":
            geometry_factory = osmium.geom.GeoJSONFactory()

        # Extract filters and parse them properly
        apply_empty_tag_filter: bool = self.options.get("emptyTagFilter", "True").lower() == "true"

        # Parse keyFilter option, default to None if not provided
        key_filter: Optional[str] = self.options.get("keyFilter", None)

        # Parse tagFilter option from a flat string to a list of tuples
        tag_filter_str: Optional[str] = self.options.get("tagFilter", None)
        tag_filter: Optional[Tuple[str, str]] = None
        if tag_filter_str:
            try:
                tag_filter = ast.literal_eval(tag_filter_str)
                if not isinstance(tag_filter, tuple) or len(tag_filter) != 2:
                    raise ValueError("tagFilter must be a tuple-like string (e.g., \"('key', 'value')\").")
            except (ValueError, SyntaxError) as e:
                raise ValueError(f"Invalid tagFilter format: {tag_filter_str}. Error: {e}")

        # Setup processor with filters
        processor = osmium.FileProcessor(input_path)
        if apply_empty_tag_filter:
            processor = processor.with_filter(osmium.filter.EmptyTagFilter())
        if key_filter:
            processor = processor.with_filter(osmium.filter.KeyFilter(key_filter))
        if tag_filter:
            processor = processor.with_filter(osmium.filter.TagFilter(tag_filter))

        # Yield the geometry data
        for element in processor.with_areas():
            geometry: Optional[str] = None
            tags: Dict[str, str] = {}
            try:
                # Generate geometry based on the type of element
                if element.is_node():
                    geometry = geometry_factory.create_point(element.location)
                elif element.is_way() and not element.is_closed():
                    geometry = geometry_factory.create_linestring(element.nodes)
                elif element.is_area():
                    geometry = geometry_factory.create_multipolygon(element)

                # Extract tags for the element
                tags = {tag.k: tag.v for tag in element.tags}
            except Exception as e:
                print(f"Error processing element {element.id}: {e}")
                continue

            yield (element.id, element.type_str(), geometry, tags)

class LasToArrayDataSource(DataSource):
    """
    A custom data source to convert OSM PBF files to geometries in WKT, WKB, or GeoJSON format,
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
        """
        return StructType([
            StructField("id", LongType(), True),
            StructField("type", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("tags", MapType(StringType(), StringType()), True)
        ])

    def reader(self, schema: StructType) -> LasToArrayDataSourceReader:
        """
        Create a data source reader for reading the PBF file.

        Args:
            schema (StructType): The schema of the output data.

        Returns:
            PBFToGeometryDataSourceReader: An instance of the data source reader.
        """
        return LasToArrayDataSourceReader(schema, self.options)

def register_las_data_source():
    if os.getenv("IS_SERVERLESS") == "TRUE":
        raise RuntimeError(
            "Error: This data source can only be executed in a non-serverless context. "
            "Please attach the notebook to a traditional compute cluster and try again."
        )
    
    spark = SparkSession.getActiveSession()
    try:
        spark.dataSource.register(LasToArrayDataSource)
        print("Custom data source 'las' registered successfully.")
    except AttributeError:
        print("Error registering custom data source: PySpark custom data sources are not supported in this environment.")
