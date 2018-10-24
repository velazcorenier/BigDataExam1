spark = SparkSession.builder
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
escuelasdf = sqlContext.read.csv("/home/osboxes/exam1-sp17-bigdata-desc/hive/escuelasPR.csv")
escuelasdf.show()
escuelascoldf = escuelasdf.selectExpr("_c0 as region", "_c1 as district", "_c2 as city", "_c3 as id_school", "_c4 as school_name","_c5 as school_lvl", "_c6 as series_college")
escuelascoldf.show()
arecibodf = escuelascoldf.filter("region='Arecibo'")
arecibodf.show()
districtGB = arecibodf.groupby('district','city')
districtGB.count().show()

