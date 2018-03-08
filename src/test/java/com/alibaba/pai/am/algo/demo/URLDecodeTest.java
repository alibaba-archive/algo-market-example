package com.alibaba.pai.am.algo.demo;

import com.aliyun.odps.udf.UDF;
import org.junit.Test;

/**
 *
 * URL用UTF-8解码

 * @author guotao.gt

 * @since 2018.03.08

 */

public class URLDecodeTest extends UDF {

    @Test
    public void test(){
        String url0 = "https%3a%2f%2falgo.alibaba-inc.com%3fk%3d%e7%ae%97%e6%b3%95%e5%b8%82%e5%9c%ba";
        System.out.println(new URLDecode().evaluate(url0));
        System.out.println(new URLDecode().evaluate(url0,"UTF-8"));
    }

}

