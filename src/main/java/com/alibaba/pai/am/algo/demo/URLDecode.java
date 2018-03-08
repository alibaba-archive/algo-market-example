package com.alibaba.pai.am.algo.demo;

import com.aliyun.odps.udf.UDF;

import java.net.URLDecoder;

/**
 *
 * URL解码

 * @author guotao.gt

 * @since 2016.11.08

 */

public class URLDecode extends UDF {

    /**
     * 用UTF-8解码
     * @param s
     * @return
     */
    public String evaluate(String s) {
        try {
            return URLDecoder.decode(s,"UTF-8");
        }catch (Exception e){
            return null;
        }
    }

    /**
     * 用指定编码解码
     * @param s
     * @param decode
     * @return
     */
    public String evaluate(String s,String decode) {
        try {
            return URLDecoder.decode(s,decode);
        }catch (Exception e){
            return null;
        }
    }

}

