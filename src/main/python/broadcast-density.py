import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

input_path = sys.argv[1]
output_path = sys.argv[2]
cell0 = float(sys.argv[3])
cell2 = cell0 * 0.5
pop = int(sys.argv[4])

if __name__ == "__main__":

    conf = SparkConf().setAppName("Broadcast Density")
    sc = SparkContext(conf=conf)
    try:
        sqlContext = SQLContext(sc)

        df = sqlContext.read \
            .format('com.esri.battuta.fgb') \
            .options(fields='the_geom', cql='Status > 0') \
            .load(input_path)

        df.registerTempTable("POINTS")

        sql = '''
              select c*{cell0}+{cell2} as lon,r*{cell0}+{cell2} as lat,count(1) as pop from
              (select cast(the_geom['x']/{cell0} as int) as c, cast(the_geom['y']/{cell0} as int) as r from POINTS) as T
              group by c,r having pop>{pop}
            '''.format(cell0=cell0, cell2=cell2, pop=pop)

        sqlContext.sql(sql).write.json(output_path)

    finally:
        sc.stop()
