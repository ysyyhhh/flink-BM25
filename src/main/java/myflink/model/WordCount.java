package myflink.model;

import java.io.Serializable;

public class WordCount implements Serializable {
    private String word;
    private long count;

    public WordCount() {
    }

    public WordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}