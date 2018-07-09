package com.aliyun.odps.spark.examples;

/**
 * Created by liwei.li on 9/1/16.
 */

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaOdpsOps;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction3;
import scala.Tuple2;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class JavaReadAndSave {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaReadAndSave");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);

        SparkSession sparkSession = SparkSession.builder().appName("spark sql test").getOrCreate();
        //String projectName = sparkConf.get("odps.project.name");
        sparkSession.sql("CREATE TABLE IF NOT EXISTS sre_mpi_algo_dev.wc_in(key STRING,value STRING)");
        //sparkSession.stop();


        //1.generate data
        List<String> sourceData = new ArrayList<String>();
        sourceData.add("a");
        sourceData.add("b");
        sourceData.add("c");
        sourceData.add("d");
        sourceData.add("f");

        //2.save to table
        JavaPairRDD<String, String> rdd1 = ctx.parallelize(sourceData, 2).mapToPair(
            new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) {
                    return new Tuple2<String, String>(s, s);
                }
            });
        VoidFunction3<Tuple2<String, String>, Record, TableSchema> saveTransfer =
            new VoidFunction3<Tuple2<String, String>, Record, TableSchema>() {
                @Override
                public void call(Tuple2<String, String> v1, Record v2, TableSchema v3) throws Exception {
                    System.out.println("value1=" + v1._1() + ",value2=" + v1._2());
                    v2.set(0, v1._1());
                    v2.set(1, v1._2());
                }
            };
        javaOdpsOps.saveToTable("sre_mpi_algo_dev", "wc_in", "", JavaPairRDD.toRDD(rdd1), saveTransfer, false);

        //3. read from table
        JavaRDD<String> resultRDD =
            javaOdpsOps.readTable(
                "sre_mpi_algo_dev", "wc_in",
                new Function2<Record, TableSchema, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(Record v1, TableSchema v2) throws Exception {
                        return new Tuple2<String, String>(v1.getString(0), v1.getString(1));
                    }
                }, 0
            ).map(new Function<Tuple2<String, String>, String>() {
                @Override
                public String call(Tuple2<String, String> v) throws Exception {
                    return "value1=" + v._1() + ",value2=" + v._2();
                }
            });
        List<String> result = resultRDD.collect();
        System.out.println("result.size = " + result.size());
        for (String item : result) {
            System.out.println(item);
        }
        ctx.stop();
    }
}
