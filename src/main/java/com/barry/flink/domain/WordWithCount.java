package com.barry.flink.domain;

import com.barry.flink.WordCount;

/**
 * @ClassName WordWithCount
 * @Author wangxuexing
 * @Date 2020/7/11 22:15
 * @Version 1.0
 */
public class WordWithCount {
    public String word;
    public long count;
    public WordWithCount(){}
    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return WordCount.getDateTime(System.currentTimeMillis())+"WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
