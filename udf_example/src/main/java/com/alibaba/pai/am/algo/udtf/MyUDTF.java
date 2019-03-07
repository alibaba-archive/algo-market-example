package com.alibaba.pai.am.algo.udtf;

import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.UDFException;
// TODO define input and output types, e.g., "string,string->string,bigint".
@Resolve("string,bigint->string,bigint")
public class MyUDTF extends UDTF {
    @Override
    public void process(Object[] args) throws UDFException {
        String a = (String) args[0];
        Long b = (Long) args[1];
        for (String t: a.split("\\s+")) {
            forward(t, b);
        }
    }
}

// input
//          +------+------+
//          | col0 | col1 |
//          +------+------+
//          | A B  | 1    |
//          | C D  | 2    |
//          +------+------+

// output
//               +----+----+
//               | c0 | c1 |
//               +----+----+
//               | A  | 1  |
//               | B  | 1  |
//               | C  | 2  |
//               | D  | 2  |
//               +----+----+