#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark sql").getOrCreate()

    df = spark.sql("select * from dual")
    df.printSchema()
    df.show(1, 1)

    #Create Drop Table
    spark.sql("drop table if exists srcp").show
    spark.sql("create table if not exists srcp (key string ,value bigint) partitioned by (p string)").show
    spark.sql("insert into table srcp partition (p='abc') values ('a',1),('b',2),('c',3)").show
