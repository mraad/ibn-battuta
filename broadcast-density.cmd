@ECHO OFF
spark-submit.cmd^
 --master "local[*]"^
 --driver-memory 1G^
 --executor-memory 4G^
 --repositories http://repo.boundlessgeo.com/main^
 --packages org.geotools:gt-ogr-bridj:14-SNAPSHOT,org.geotools:gt-cql:14-SNAPSHOT^
 --conf spark.driver.extraJavaOptions="-DGDAL_LIBRARY_NAME=gdal111"^
 --driver-library-path C:\OSGeo4W64\bin^
 --jars target\ibn-battuta-0.2.jar^
 src\main\python\broadcast-density.py^
 C:\temp\Miami.gdb\Broadcast C:\temp\output 0.001 10
