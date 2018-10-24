spark = SparkSession.builder
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
escuelasdf = sqlContext.read.csv("/home/osboxes/exam1-sp17-bigdata-desc/hive/escuelasPR.csv")
escuelasdf.show()
escuelascoldf = escuelasdf.selectExpr("_c0 as region", "_c1 as district", "_c2 as city", "_c3 as id_school", "_c4 as school_name","_c5 as school_lvl", "_c6 as series_college")
escuelascoldf.show()
studentsdf = sqlContext.read.csv("/home/osboxes/exam1-sp17-bigdata-desc/hive/studentsPR.csv")
studentsdf.show()
studentscoldf = studentsdf.selectExpr("_c0 as region", "_c1 as  district", "_c2 as id_school", "_c3 as school_name", "_c4 as stud_lvl", "_c5 as gender", "_c6 as id_student")
studentscoldf.show()
cond = escuelascoldf.id_school == studentscoldf.id_school
joineddf = studentscoldf.join(escuelascoldf,cond)
joineddf.show()
resultsdf = joineddf.filter("gender='M' and school_lvl = 'Superior'").filter("city = 'Ponce' or city = 'San Juan'")
resultsdf.show() 
resultsdf.count()

