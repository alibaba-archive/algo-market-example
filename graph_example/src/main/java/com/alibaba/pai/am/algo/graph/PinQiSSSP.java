package com.alibaba.pai.am.algo.graph;


import com.alibaba.fastjson.JSON;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.*;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.WritableRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author 品七
 */
public class PinQiSSSP {
    public static final String START_VERTEX = "sssp.start.vertex.id";
    public static class SSSPVertex extends
            Vertex<LongWritable, LongWritable, LongWritable, LongWritable> {
        private static long startVertexId = -1;
        public SSSPVertex() {
            this.setValue(new LongWritable(Long.MAX_VALUE));
        }
        public boolean isStartVertex(
                ComputeContext<LongWritable, LongWritable, LongWritable, LongWritable> context) {
            if (startVertexId == -1) {
                String s = context.getConfiguration().get(START_VERTEX);
                startVertexId = Long.parseLong(s);
            }
            return getId().get() == startVertexId;
        }
        @Override
        public void compute(
                ComputeContext<LongWritable, LongWritable, LongWritable, LongWritable> context,
                Iterable<LongWritable> messages) throws IOException {
            long minDist = isStartVertex(context) ? 0 : Integer.MAX_VALUE;
            for (LongWritable msg : messages) {
                if (msg.get() < minDist) {
                    minDist = msg.get();
                }
            }
            if (minDist < this.getValue().get()) {
                this.setValue(new LongWritable(minDist));
                if (hasEdges()) {
                    for (Edge<LongWritable, LongWritable> e : this.getEdges()) {
                        context.sendMessage(e.getDestVertexId(), new LongWritable(minDist
                                + e.getValue().get()));
                    }
                }
            } else {
                voteToHalt();
            }
        }
        @Override
        public void cleanup(
                WorkerContext<LongWritable, LongWritable, LongWritable, LongWritable> context)
                throws IOException {
            context.write(getId(), getValue());
        }
    }
    public static class MinLongCombiner extends
            Combiner<LongWritable, LongWritable> {
        @Override
        public void combine(LongWritable vertexId, LongWritable combinedMessage,
                            LongWritable messageToCombine) throws IOException {
            if (combinedMessage.get() > messageToCombine.get()) {
                combinedMessage.set(messageToCombine.get());
            }
        }
    }
    public static class SSSPVertexReader extends
            GraphLoader<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        public void load(
                LongWritable recordNum,
                WritableRecord record,
                MutationContext<LongWritable, LongWritable, LongWritable, LongWritable> context)
                throws IOException {
            SSSPVertex vertex = new SSSPVertex();
            vertex.setId((LongWritable) record.get(0));
            String[] edges = record.get(1).toString().split(",");
            for (int i = 0; i < edges.length; i++) {
                String[] ss = edges[i].split(":");
                vertex.addEdge(new LongWritable(Long.parseLong(ss[0])),
                        new LongWritable(Long.parseLong(ss[1])));
            }
            context.addVertexRequest(vertex);
        }
    }

    /**
     *  只在用户本地测试时有效，同MR一样，实际的main函数由算法市场的SDK自动拼装生成
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
//        if (args.length < 2) {
//            System.out.println("Usage: <startnode> <input> <output>");
//            System.exit(-1);
//        }
        Properties props = System.getProperties();
        props.setProperty("odps.project.name", "xx");
        props.setProperty("odps.end.point", "xxx");
        props.setProperty("odps.classpath.resources", "xxx");
        props.setProperty("odps.account.provider", "aliyun");
        props.setProperty("odps.access.id", "xxx");
        props.setProperty("odps.access.key", "xxx=");
        props.setProperty("odps.runner.mode", "remote");
        GraphJob job = new GraphJob();
        job.setGraphLoaderClass(SSSPVertexReader.class);
        job.setVertexClass(SSSPVertex.class);
        job.setCombinerClass(MinLongCombiner.class);
        job.set(START_VERTEX, "0");
        job.addInput(TableInfo.builder().tableName("dual_mulit_column").build());
        job.addOutput(TableInfo.builder().tableName("dual_column_out").build());
        Map<String, String> hint = new HashMap<String, String>();
        Iterator<Map.Entry<String, String>> itr = job.iterator();
        while (itr.hasNext()) {
            Map.Entry<String, String> e = itr.next();
            hint.put(e.getKey(), e.getValue());
        }
        System.out.println(JSON.toJSONString(hint));
        long startTime = System.currentTimeMillis();
        job.run();
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
}
