# Lidar example (LAZ/LAS file processing)


I think we should look to refresh LAZ file example, possibly even introduce a python datasource to simply extract the contents of point cloud into a table. Then use the built-in power of databricks to work with the points. This could go under the spatial-utils branch for now. As far as libraries, PDAL is the perennial go-to; however, we may want to branch out to avoid the build / env issues that arise (not the least of which is matching with GDAL dependency). See Timo's very recent OSM datasource example as a reference using spark.read.format("pbf").

We should consider incorporating laspy [+lazrs +laszip-python] for las/laz format and plyfile for ply format (introducing readers for each format). These libs seem to have minimal dependencies (and no PDAL requirement!).

https://laspy.readthedocs.io/en/latest/installation.html

https://python-plyfile.readthedocs.io/en/latest/install.html

When encountering LAZ data, laspy will try to use one of the backend in the order described above. (Example: if lazrs is not installed or if it fails during, the process, laspy will try laszip)

lazrs is a Rust port of the laszip compression and decompression. Its main advantage is that it is able to compress/decompress using multiple threads which can greatly speed up things. However it does not supports points with waveforms.

laszip is the official and original LAZ implementation by Martin Isenburg. The advantage of the laszip backend is that its the official implementation, it supports points with waveform but does not offer multi-threaded compression/decompression.

classlaspy.compression.LazBackend(value)[source]
Bases: ILazBackend, Enum

Supported backends for reading and writing LAS/LAZ

LazrsParallel= 0
lazrs in multi-thread mode

Lazrs= 1
lazrs in single-thread mode

Laszip= 2
laszip backend