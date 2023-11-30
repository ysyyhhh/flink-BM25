package myflink;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import myflink.model.TF;
import myflink.util.MongoUtil;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MongoIDFSink extends RichOutputFormat<ConcurrentHashMap<String,Long>>{

    private MongoUtil mongoUtil = MongoUtil.instance;
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void writeRecord(ConcurrentHashMap<String, Long> stringLongConcurrentHashMap) throws IOException {
        TF tf = new TF(1130,stringLongConcurrentHashMap);
        MongoCollection<Document> coll = mongoUtil.getCollection("candidate","idf");
        //如果已经存在,则更新
        Document doc = tf.toDoc();

        BasicDBObject searchDoc = new BasicDBObject().append("_id",tf.pid);
        BasicDBObject newDoc = new BasicDBObject().append("$set",doc);

        coll.findOneAndUpdate(searchDoc,newDoc,new FindOneAndUpdateOptions().upsert(true));
    }

    @Override
    public void close() throws IOException {

    }
}
