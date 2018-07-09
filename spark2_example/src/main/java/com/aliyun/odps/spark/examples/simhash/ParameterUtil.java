package com.aliyun.odps.spark.examples.simhash;

import java.util.HashMap;
import java.util.Map;

public class ParameterUtil {

    public static final String delimiter = "=";

    /**
     * 转化输入参数
     * @param args
     * @return
     */
    public static Map<String, String> getMainArgs(String args[]) {
        System.out.println("============Parameter================");
        Map<String,String> params = new HashMap<>();
        for(String arg:args){
            String ss [] = arg.trim().split(delimiter);
            params.put(ss[0],ss[1]);
            System.out.println(ss[0]+":"+ss[1]);
        }
        System.out.println("=====================================");
        return params;
    }
}
