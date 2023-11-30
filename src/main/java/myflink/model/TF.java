package myflink.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
public class TF {
    public Integer pid;

    public List<Word> tfList;

    public TF(int pid, ConcurrentHashMap<String, Long> stringLongConcurrentHashMap) {
        this.pid = pid;
        this.tfList = new ArrayList<>();
        for (String s : stringLongConcurrentHashMap.keySet()) {
            this.tfList.add(new Word(s,stringLongConcurrentHashMap.get(s)));
        }
        //排序
        this.tfList.sort((o1, o2) -> {
            if(o1.count>o2.count){
                return -1;
            }else if(o1.count<o2.count){
                return 1;
            }else{
                return 0;
            }
        });
    }

    class Word{
        public String word;
        public Long count;

        public Word(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        public Document toDoc(){
            return new Document("w",word).append("c",count);
        }


    }
    public TF(Integer pid, DataSet<Tuple2<String, Long>> tfList) throws Exception {
        this.pid = pid;
        this.tfList = new ArrayList<>();
        List<Tuple2<String, Long>>  tfList1 = tfList.collect();
        for (Tuple2<String, Long> tuple2 : tfList1) {
            this.tfList.add(new Word(tuple2.f0,tuple2.f1));
        }
    }
    public TF(Integer pid, List<Tuple2<String, Long>> tfList) throws Exception {
        this.pid = pid;
        this.tfList = new ArrayList<>();
        for (Tuple2<String, Long> tuple2 : tfList) {
            this.tfList.add(new Word(tuple2.f0,tuple2.f1));
        }
    }

    public Document toDoc(){
        Document doc = new Document("_id",pid);
        List<Document> list = new ArrayList<>();
        for (Word word : tfList) {
            list.add(word.toDoc());
        }
        doc.append("tf",list);
        return doc;
    }

}
