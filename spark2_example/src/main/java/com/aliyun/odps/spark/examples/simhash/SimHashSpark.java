package com.aliyun.odps.spark.examples.simhash;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaOdpsOps;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction3;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * @author guotao.gt
 */
public class SimHashSpark {

    /**
     * algo param
     */
    static class SimHashParam {
        public static String INPUT_TABLE = "inputTable";
        public static String ID_COL = "idCol";
        public static String CONTENT_COL = "contentCol";
        public static String OUTPUT_TABLE = "outputTable";
        public static String BIT_LENGTH = "bitLength";
        public static String RADIX = "radix";

    }

    public static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static void main(String[] args) throws Exception {

        final String inputProjectName;
        final String inputTableName;
        final String idCol;
        final String contentCol;
        final String outputProjectName;
        final String outputTableName;
        final int bitLength;
        final int radix;

        // initial Ctx
        SparkConf sparkConf = new SparkConf().setAppName("SimHashSpark");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
        String projectName = sparkConf.get("spark.hadoop.odps.project.name");

        //initial params
        Map<String, String> params = ParameterUtil.getMainArgs(args);

        if (null != params.get(SimHashParam.INPUT_TABLE)) {
            String[] inputs = params.get(SimHashParam.INPUT_TABLE).split("\\.");
            if (inputs.length == 2) {
                inputProjectName = inputs[0];
                inputTableName = inputs[1];
            } else if (inputs.length == 1) {
                inputProjectName = projectName;
                inputTableName = inputs[0];
            } else {
                throw new RuntimeException("inputTable format should be projectName.tableName or tableName");
            }
        } else {
            throw new RuntimeException("outputTable should not be null");
        }

        if (null != params.get(SimHashParam.OUTPUT_TABLE)) {
            String[] outputs = params.get(SimHashParam.OUTPUT_TABLE).split("\\.");
            if (outputs.length == 2) {
                outputProjectName = outputs[0];
                outputTableName = outputs[1];
            } else if (outputs.length == 1) {
                outputProjectName = projectName;
                outputTableName = outputs[0];
            } else {
                throw new RuntimeException("outputTable format should be projectName.tableName or tableName");
            }
        } else {
            throw new RuntimeException("outputTable should not be null");
        }

        if (null != params.get(SimHashParam.ID_COL)) {
            idCol = params.get(SimHashParam.ID_COL);
        } else {
            throw new RuntimeException("idCol should not be null");
        }

        if (null != params.get(SimHashParam.CONTENT_COL)) {
            contentCol = params.get(SimHashParam.CONTENT_COL);
        } else {
            throw new RuntimeException("contentCol should not be null");
        }

        if (null != params.get(SimHashParam.BIT_LENGTH)) {
            bitLength = Integer.valueOf(params.getOrDefault(SimHashParam.BIT_LENGTH, "128"));
        } else {
            bitLength = 128;
        }

        if (null != params.get(SimHashParam.RADIX)) {
            radix = Integer.valueOf(params.getOrDefault(SimHashParam.RADIX, "10"));
        } else {
            radix = 10;
        }

        // create output table
        SparkSession sparkSession = SparkSession.builder().appName("spark sql test").getOrCreate();
        sparkSession.sql("CREATE TABLE IF NOT EXISTS " + outputProjectName + "." + outputTableName+"(id STRING,hash_value STRING)");
        //sparkSession.stop();

        //1. read from table
        JavaRDD<Sample> inputRDD =
            javaOdpsOps.readTable(
                inputProjectName, inputTableName,
                new Function2<Record, TableSchema, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(Record v1, TableSchema v2) throws Exception {
                        System.out.println("TableSchema v2:" + gson.toJson(v2));
                        return new Tuple2<String, String>(v1.getString(idCol), v1.getString(contentCol));
                    }
                }, 0
            ).map(new Function<Tuple2<String, String>, Sample>() {
                @Override
                public Sample call(Tuple2<String, String> v) throws Exception {
                    return new Sample(v._1(), v._2());
                }
            });

        List<Sample> inputSamples = inputRDD.collect();

        //test
        System.out.println("inputSamples size = " + inputSamples.size());
        System.out.println("record 0: "+inputSamples.get(0));

        //2.save to table
        JavaPairRDD<String, String> rdd1 = ctx.parallelize(inputSamples, 1).mapToPair(
            new PairFunction<Sample, String, String>() {
                @Override
                public Tuple2<String, String> call(Sample sample) throws Exception {
                    String simHash = SimHash.simHash(sample.getContent(), bitLength, radix);
                    return new Tuple2<String, String>(sample.getId(), simHash.toString());
                }
            }
        );

        VoidFunction3<Tuple2<String, String>, Record, TableSchema> saveTransfer =
            new VoidFunction3<Tuple2<String, String>, Record, TableSchema>() {
                @Override
                public void call(Tuple2<String, String> v1, Record v2, TableSchema v3) throws Exception {
                    v2.set(0, v1._1());
                    v2.set(1, v1._2());
                }
            };
        javaOdpsOps.saveToTable(outputProjectName, outputTableName, "", JavaPairRDD.toRDD(rdd1), saveTransfer, true);

        ctx.stop();
    }

}