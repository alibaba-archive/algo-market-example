package com.alibaba.pai.spark.preprocess;

import java.util.Map;
import java.util.HashMap;
import com.alibaba.pai.spark.preprocess.api.Context;
import com.alibaba.pai.spark.preprocess.api.PreprocessInterface;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;


public class Main implements PreprocessInterface {

    Map<String, String> algoParam;
    Map<String, String> sparkConfig;

    public Main() {
        algoParam = new HashMap<String, String>();
        sparkConfig = new HashMap<String, String>();
    }
    
    public void Process(Context ctx, Map<String, String> algoParam) throws Exception {
        this.algoParam.putAll(algoParam);

        if (!algoParam.containsKey("inputTableName")) {
            throw new Exception("Mssing Parameter inputTableName.");
        }

        if (!algoParam.containsKey("outputTableName")) {
            throw new Exception("Mssing Parameter outputTableName.");
        }

        if (!algoParam.containsKey("fromNodeCol")) {
            throw new Exception("Mssing Parameter fromNodeCol.");
        }

        if (!algoParam.containsKey("toNodeCol")) {
            throw new Exception("Mssing Parameter toNodeCol.");
        }

        String inputTableName = algoParam.get("inputTableName");
        String outputTableName = algoParam.get("outputTableName");

        if (ctx.odps().tables().exists(outputTableName)) {
            throw new Exception(String.format("the output table[%s] is exist.", outputTableName));
        }

//        check column
        String fromNodeCol = algoParam.get("fromNodeCol");
        String toNodeCol = algoParam.get("toNodeCol");

        Table inTable = ctx.odps().tables().get(inputTableName);
        if (!inTable.getSchema().containsColumn(fromNodeCol)) {
            throw new Exception(String.format(
                    "The column[%s] is not exist in Table[%s].", fromNodeCol, inputTableName));
        }

        if (!inTable.getSchema().containsColumn(toNodeCol)) {
            throw new Exception(String.format(
                    "The column[%s] is not exist in Table[%s].", toNodeCol, inputTableName));
        }

        OdpsType nodeColType = inTable.getSchema().getColumn(fromNodeCol).getTypeInfo().getOdpsType();
        if (inTable.getSchema().getColumn(toNodeCol).getTypeInfo().getOdpsType() != nodeColType) {
            throw new Exception(String.format(
                    "the type of column[%s] should be same with column[%s].",
                    fromNodeCol, toNodeCol));
        }

//        create outputTable
        TableSchema outTableSchema = new TableSchema();
        outTableSchema.addColumn(new Column("node", nodeColType));
        outTableSchema.addColumn(new Column("degree", OdpsType.BIGINT));

        if (algoParam.containsKey("lifecycle")) {
            ctx.odps().tables().createTableWithLifeCycle(
                    ctx.odps().getDefaultProject(),
                    outputTableName, outTableSchema, "", false,
                    Long.parseLong(algoParam.get("lifecycle")));
        } else {
            ctx.odps().tables().create(outputTableName, outTableSchema);
        }

//        calc quota
        long totRecordCnt = ctx.getRecordCount(inputTableName);
        long instCnt = totRecordCnt / (2 * 1000 * 1000);

        if (instCnt > 1000) {
            instCnt = 1000;
        }
        if (instCnt < 1) {
            instCnt = 1;
        }

        this.algoParam.put("numPartitions", Long.toString(instCnt*2));

        sparkConfig.put("spark.driver.cores", "2");
        sparkConfig.put("spark.driver.memory", "4g");
        sparkConfig.put("spark.executor.cores", "2");
        sparkConfig.put("spark.executor.instances", Long.toString(instCnt));
        sparkConfig.put("spark.executor.memory", "8g");
    }

    public Map<String, String> GetAlgoParams() {
        return algoParam;
    }

    public Map<String, String> GetSparkConfig() {
        return sparkConfig;
    }
}
