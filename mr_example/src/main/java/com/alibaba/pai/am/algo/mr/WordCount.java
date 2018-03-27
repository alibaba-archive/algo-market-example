package com.alibaba.pai.am.algo.mr;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import java.io.IOException;
import java.util.Iterator;

public class WordCount {
    public static class TokenizerMapper extends MapperBase {
        private Record word;
        private Record one;

        @Override
        public void setup(TaskContext context) throws IOException {
            word = context.createMapOutputKeyRecord();
            one = context.createMapOutputValueRecord();
            one.set(new Object[] { 1L });

            System.out.println("TaskID:" + context.getTaskID().toString());
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context)
                throws IOException {
            for (int i = 0; i < record.getColumnCount(); i++) {
                word.set(new Object[] { record.get(i).toString() });
                context.write(word, one);
            }
        }
    }

    /**
     * A combiner class that combines map output by sum them.
     **/
    public static class SumCombiner extends ReducerBase {
        private Record count;

        @Override
        public void setup(TaskContext context) throws IOException {
            count = context.createMapOutputValueRecord();
        }

        @Override
        public void reduce(Record key, Iterator<Record> values,
                           TaskContext context) throws IOException {
            long c = 0;
            while (values.hasNext()) {
                Record val = values.next();
                c += (Long) val.get(0);
            }
            count.set(0, c);
            context.write(key, count);
        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     **/
    public static class SumReducer extends ReducerBase {
        private Record result = null;

        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }

        @Override
        public void reduce(Record key, Iterator<Record> values,
                           TaskContext context) throws IOException {
            long count = 0;
            //取算法参数
            String threshold = context.getJobConf().get(
                    "odps.wordcount.min_cnt", "unset");
            int threshold_int = Integer.parseInt(threshold);
            while (values.hasNext()) {
                Record val = values.next();
                count += (Long) val.get(0);
            }
            if (count > threshold_int) {
                result.set(0, key.get(0));
                result.set(1, count);
                context.write(result);
            }
        }
    }

    /**
     * 只在用户本地测试时有效，实际生成main函数的方法见(http://gitlab.alibaba-inc.com/algo_market/doc/wikis/how_to_develop_MR_aglorithms_for_algomarket)第10项说明。
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <in_table> <out_table>");
            System.exit(2);
        }

        JobConf job = new JobConf();

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

        InputUtils
                .addTable(TableInfo.builder().tableName(args[0]).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
                job);

        JobClient.runJob(job);
    }
}