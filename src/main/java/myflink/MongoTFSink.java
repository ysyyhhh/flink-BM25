package myflink;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import myflink.model.TF;
import myflink.util.MongoUtil;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class MongoTFSink extends RichOutputFormat<ArrayList<Tuple2<String,Long>> >{
    public static final ConfigOption<? super Integer> CONFIG_KEY = ConfigOptions.key("key").defaultValue(0).withDescription("key");

    private Integer pid;

    private MongoUtil mongoUtil = MongoUtil.instance;
    public MongoTFSink(Integer pid){
        super();
        this.pid = pid;

    }
    @Override
    public void configure(Configuration configuration) {


    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void writeRecord(ArrayList<Tuple2<String, Long>> tuple2s) throws IOException {
        TF tf = null;
        try {
            tf = new TF(pid,tuple2s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MongoCollection<Document> coll = mongoUtil.getCollection("candidate","tf4");

        Document doc = tf.toDoc();

        BasicDBObject searchDoc = new BasicDBObject().append("_id",tf.pid);
        BasicDBObject newDoc = new BasicDBObject().append("$set",doc);

        coll.findOneAndUpdate(searchDoc, newDoc, new FindOneAndUpdateOptions().upsert(true));
    }

    @Override
    public void close() throws IOException {

    }
}
