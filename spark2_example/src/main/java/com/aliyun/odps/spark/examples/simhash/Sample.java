package com.aliyun.odps.spark.examples.simhash;

import java.io.Serializable;

/**
 * @author guotao.gt
 */
public class Sample implements Serializable {
    private String id;
    private String content;
    private String hashValue;

    public Sample() {
    }

    public Sample(String id, String content) {
        this.id = id;
        this.content = content;
    }

    public Sample(String id, String content, String hashValue) {
        this.id = id;
        this.content = content;
        this.hashValue = hashValue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getHashValue() {
        return hashValue;
    }

    public void setHashValue(String hashValue) {
        this.hashValue = hashValue;
    }

    @Override
    public String toString() {
        return "Sample{" +
            "id='" + id + '\'' +
            ", content='" + content + '\'' +
            ", hashValue='" + hashValue + '\'' +
            '}';
    }
}