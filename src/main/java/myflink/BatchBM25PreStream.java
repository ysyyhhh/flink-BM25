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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.bson.Document;
import scala.Int;

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
public class BatchBM25PreStream {



    private MongoUtil mongoUtil = MongoUtil.instance;

    // 词性
    private static Set<String> expectedNature = new HashSet<String>() {{
        add("n");
        add("nr");
        add("ns");
        add("nt");
        add("nz");
        add("nw");
        add("nl");
        add("ng");
//                add("t");
//                add("tg");
//                add("s");
//                add("f");
    }};

    // 停用词
    private static Set<String> stopWords = new HashSet<>();
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

    public static void main(String[] args) throws Exception {
        new BatchBM25PreStream().execute(args);
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

            // 设置运行模式
            env.setParallelism(8);

            List<Integer> pidList = getPidList();

            System.out.println("pidList.size(): " + pidList.size());

            int count = 0;
            for(Integer pid : pidList){

                count++;
                if(count % 50 == 0)
                    System.out.println("count: " + count);
                if(count > limit){
                    break;
                }

                //获取单个案件信息
                Tuple2<Integer,String> source = getByPid(pid);

                //案件内容
                String s = source.f1;

                // 1. 获取数据源
                DataSet<String> elementsSource = env.fromElements(s);



                // 第一步，对String进行分词，得到ArrayList, Tuple2<String,String>> ("词","词性")
                DataSet<ArrayList<Tuple2<String, String>> > splitOperator = elementsSource.flatMap(new FlatMapFunction<String, ArrayList<Tuple2<String, String>>>() {
                    @Override
                    public void flatMap(String value, Collector<ArrayList<Tuple2<String, String>>> out) throws Exception {

                        ArrayList<Tuple2<String, String>> set = new ArrayList<>();
                        //统计单词使用频次，放入收集器
                        Result result = ToAnalysis.parse(value); //分词结果的一个封装，主要是一个Term>的terms
                        for (Term term : result.getTerms()) {
                            String word = term.getName(); //拿到词
                            String natureStr = term.getNatureStr(); //拿到词性
                            //过滤掉词性不是名词的词,和停用词
                            boolean flag = expectedNature.contains(natureStr) && !stopWords.contains(word);
                            if (flag)
                                set.add(new Tuple2<>(word, natureStr));
                        }
                        out.collect(set);
                    }
                });

                //第二步，对ArrayList<Tuple2<String, String>>进行去重，得到ConcurrentHashMap<String, String> ("词","词性")
                DataSet<ConcurrentHashMap<String, Long>> distinctOperator = splitOperator.map(new MapFunction<ArrayList<Tuple2<String, String>>, ConcurrentHashMap<String, Long>>() {
                    @Override
                    public ConcurrentHashMap<String, Long> map(ArrayList<Tuple2<String, String>> value) throws Exception {
                        ConcurrentHashMap<String, Long> wordMap = new ConcurrentHashMap<>(16);
                        for (Tuple2<String, String> tuple2 : value) {
                            String word = tuple2.f0;
                            Long count = wordMap.get(word);
                            if (count == null) {
                                count = 0L;
                            }
                            count++;
                            wordMap.put(word, count);
                        }
                        return wordMap;
                    }
                });

                //第三步，对ConcurrentHashMap<String, String>进行排序，得到ArrayList<Tuple2<String, String>> ("词","词性")
                DataSet<ArrayList<Tuple2<String, Long>> > sortedOperator = distinctOperator.map(new MapFunction<ConcurrentHashMap<String, Long>, ArrayList<Tuple2<String, Long>>>() {
                    @Override
                    public ArrayList<Tuple2<String, Long>> map(ConcurrentHashMap<String, Long> value) throws Exception {
                        ArrayList<Tuple2<String, Long>> wordCountArrayList = new ArrayList<>();
                        value.forEach((key, value1) -> wordCountArrayList.add(new Tuple2<>(key, value1)));
                        //避免集合为空
                        wordCountArrayList.add(new Tuple2<>("flink", 0L));

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
                        return wordCountArrayList;
                    }
                });

                // 第四步，保存到MongoDB
                sortedOperator.output(
                        new MongoTFSink(pid)
                );

                env.execute("flink-hello-world");
            }
        }
    }
}