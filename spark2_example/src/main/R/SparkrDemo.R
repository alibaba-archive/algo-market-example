# can import other library that list in the ReadMe.md
# SparkR package is mandatory
library(SparkR)

# initialize on Spark env
sparkSession <- sparkR.session()

# read from a odps table as dataframe
results <- sql("FROM cupid_wordcount SELECT id, value")
head(results)

# register as temp table
createOrReplaceTempView(results, "results")

# insert into odps table
sql("drop table if exists spark_r_temp")

# CTAS
sql("create table spark_r_temp as select count(1) as count from results group by id")

# Insert into
sql("insert into table spark_r_temp select 123")

# Insert overwrite
sql("insert overwrite table spark_r_temp select 777")

# other SparkR APIs plz refer to http://spark.apache.org/docs/latest/sparkr.html
