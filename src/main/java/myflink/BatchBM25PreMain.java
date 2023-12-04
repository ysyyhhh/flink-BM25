package myflink;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import myflink.model.TF;
import myflink.util.MongoUtil;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于批处理的BM25预处理
 * 改成使用dataStream
 * args[0] 为 limit 表示从MongoDB中取多少条数据,默认为-1,表示取全部
 * 输出：
 *    output : 生成的tf文件夹,每个文件对应一个pid
 *       {pid}.txt
 *    idf.txt : 全局的idf文件
 */
public class BatchBM25PreMain {



    private MongoUtil mongoUtil = MongoUtil.instance;

    // 词性
    private static Set<String> expectedNature = new HashSet<String>() {{
        add("n");
        add("ns");
        add("nt");
        add("nz");
    }};

    // 停用词
    private static Set<String> stopWords = new HashSet<String>() {{
        add("中华人民共和国");
    }};

    public static final Integer MAX_WORD_COUNT = 30;
    /**
     * 获取所有pid
     */
    private List<Integer> getPidList(){
        MongoCollection<Document> collection = mongoUtil.getCollection("candidate", "file");

        // 只查询pid字段
        BasicDBObject searchDoc = new BasicDBObject().append("pid", 1);

        // 查询
        MongoCursor<Document> cursor = collection.find().projection(searchDoc).iterator();

        List<Integer> pidList = new ArrayList<>();

        while (cursor.hasNext()) {
            Document document = cursor.next();
            Integer pid = document.getInteger("pid");
            pidList.add(pid);
        }
        return pidList;
    }

    /**
     * 根据pid获取单个文档
     * @param pid
     * @return
     */
    private Tuple2<Integer,String> getByPid(Integer pid) {
        MongoCollection<Document> collection = mongoUtil.getCollection("candidate", "file");

        // 按照pid查询单个
        BasicDBObject searchDoc = new BasicDBObject().append("pid", pid);

        // 查询
        MongoCursor<Document> cursor = collection.find(searchDoc).iterator();

        Tuple2<Integer,String> qwList = null;
        while (cursor.hasNext()) {
            Document document = cursor.next();
            Integer _pid = document.getInteger("pid");
            String qw = document.getString("qw");
            String fact = document.getString("fact");
            String reason = document.getString("reason");
            String result = document.getString("result");

            //案件内容由以下四部分组成
            String content = qw + fact + reason + result;
            qwList = new Tuple2<>(_pid, content);

            // 只取第一个
            break;
        }
        return qwList;

    }

    public void saveIDF(List<Tuple2<String,Long>> idfList) throws Exception {
        MongoCollection<Document> coll = mongoUtil.getCollection("candidate","idf4");

        Document doc;
        for(Tuple2<String,Long> tuple2 : idfList){
            doc = new Document();
            String word = tuple2.f0;
            //如果能找到,则更新将对对应记录的c字段+1
            if (coll.find(new BasicDBObject().append("w",word)).first() != null) {
                coll.findOneAndUpdate(new BasicDBObject().append("w",word), new BasicDBObject().append("$inc", new BasicDBObject().append("c", 1)));
            } else {
                //如果找不到,则插入 {w:word,c:1}
                doc.append("w",word).append("c",1);
                coll.insertOne(doc);
            }
        }
    }
    DataSet<Tuple2<String, Long> > IDFSet = null;
    DataSet<ConcurrentHashMap<String, Long>> unionSet = null;
    DataSet<ArrayList<Tuple2<String, Long>>> idfList = null;
    public static void main(String[] args) throws Exception {
        new BatchBM25PreMain().execute(args);
    }

    /**
     * 执行
     * @param args
     * @throws Exception
     */
    private void execute(String[] args) throws Exception {
        {
            // 限制取多少条数据
            Integer limit = args.length > 0 ? Integer.parseInt(args[0]) : -1;

            // 准备环境
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            IDFSet = env.fromElements(new Tuple2<>("flink", 0L));

            unionSet = env.fromElements(new ConcurrentHashMap<String, Long>());
            // 设置运行模式
            env.setParallelism(8);

            //获取案件pid列表
            List<Integer> pidList = getPidList();

            if(limit == -1){
                limit = pidList.size() + 1;
            }

            if (limit > pidList.size())
                limit = pidList.size();
            pidList = pidList.subList(0,limit);

            int i = 0;
            for(Integer pid : pidList){
                i++;
                if(i % 50 == 0){
                    System.out.println("i = " + i);
                }

                //获取单个案件信息
                Tuple2<Integer,String> source = getByPid(pid);

                //案件内容
                String s = source.f1;

                // 第一步. 获取数据
                // 此时DataSet只有一个元素，就是s
                // elementsSource.count() == 1
                DataSet<String> elementsSource = env.fromElements(s);


                // 第二步，对String进行分词，得到DataSet<Tuple2<String,String>> ("词","词性")
                // 此时DataSet有多个元素，每个元素是一个Tuple2<String,String>， 整个DataSet是对s的分词结果
                // splitOperator.count() == s的分词结果的个数
                DataSet<Tuple2<String, String>> splitOperator = elementsSource.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                        Result result = ToAnalysis.parse(value);
                        List<Term> terms = result.getTerms();
                        for (Term term : terms) {
                            String word = term.getName();
                            String natureStr = term.getNatureStr();
                            if (expectedNature.contains(natureStr) && !stopWords.contains(word)) {
                                out.collect(new Tuple2<>(word, natureStr));
                            }
                        }
                    }
                });

                //第三步，splitOperator进行去重，得到DataSet<String, String> ("词","词性")
                // distinctOperator.count() == 去重后的个数
                DataSet<Tuple2<String, Long>> distinctOperator = splitOperator.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
                        return new Tuple2<>(value.f0, 1L);
                    }
                }).groupBy(0).sum(1);

                //第四步，将整个DataSet<Tuple2<String, Long>> distinctOperator合成一个 ArrayList<Tuple2<String, Long>>>
                // countOperator.count() == 1
                List<Tuple2<String, Long>> wordList = distinctOperator.collect();
                DataSet<List<Tuple2<String, Long>>> countOperator =  env.fromElements( wordList);

                //第五步，对countOperator进行排序，得到DataSet<ArrayList<Tuple2<String, Long>>>
                // sortedOperator.count() == 1
                DataSet<ArrayList<Tuple2<String, Long>> > sortedOperator = countOperator.map(new MapFunction<List<Tuple2<String, Long>>, ArrayList<Tuple2<String, Long>>>() {
                    @Override
                    public ArrayList<Tuple2<String, Long>> map(List<Tuple2<String, Long>> value) throws Exception {
                        ArrayList<Tuple2<String, Long>> wordCountArrayList = new ArrayList<>(value);
                        //根据词频排序
                        wordCountArrayList.sort((o1, o2) -> {
                            if (o1.f1 > o2.f1) {
                                return -1;
                            } else if (o1.f1 < o2.f1) {
                                return 1;
                            } else {
                                return 0;
                            }
                        });

                        //并保留前MAX_WORD_COUNT个
                        if (wordCountArrayList.size() > MAX_WORD_COUNT) {
                            wordCountArrayList = new ArrayList<>(wordCountArrayList.subList(0, MAX_WORD_COUNT));
                        }

                        return wordCountArrayList;
                    }
                });

                // 第六步，将结果保存到MongoDB中
                sortedOperator.output(
                        new MongoTFSink(pid)
                );


                // 第七步，将本次案件的词频统计结果合并到全局的IDF中
                saveIDF(wordList);

                env.execute("flink-batch-bm25-preprocess-single");
            }

        }
    }
}