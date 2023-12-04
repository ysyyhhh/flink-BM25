package myflink.backup;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import myflink.util.MongoUtil;
import org.apache.flink.core.fs.FileSystem;
import org.bson.Document;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.domain.Result;
import org.ansj.domain.Term;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于批处理的BM25预处理
 * 改成使用dataSet
 */
public class BatchBM25Pre {

    private static ConcurrentHashMap<String, Long> wordCountMap = new ConcurrentHashMap<>(16);


    private MongoUtil mongoUtil = MongoUtil.instance;
    /**
     * 条件查询：如查询id为xxxx的学生所有信息
     */
    private List<String> getInitData(){
        MongoCollection<Document> collection = mongoUtil.getCollection("candidate","file");

        // 查询前50个
         MongoCursor<Document> cursor = collection.find().limit(50).iterator();
         // 遍历
        List<String> qwList = new ArrayList<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            String qw = document.getString("qw");
            String fact = document.getString("fact");
            String reason = document.getString("reason");
            String result = document.getString("result");
//            String charge = document.getString("charge");
//            String article = document.getString("article");
            String content = qw + fact + reason + result;
            qwList.add(content);
        }
        return qwList;
    }

    private DataSet<Tuple2<String, Long>>  init(ExecutionEnvironment env) {
        List<Tuple2<String, Long>> wordCountArrayList = new ArrayList<>(wordCountMap.size());
        wordCountMap.forEach((key, value) -> wordCountArrayList.add(new Tuple2<>(key, value)));
        //避免集合为空
        if (wordCountArrayList.size() == 0) {
            wordCountArrayList.add(new Tuple2<>("flink", 0L));
        }
        return env.fromCollection(wordCountArrayList);
    }

    public static void main(String[] args) throws Exception {
        new BatchBM25Pre().execute(args);
    }

    private void executeOne(String content){

    }
    private void execute(String[] args) throws Exception {
        {

            // 准备环境
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<String, Long>> initSet = init(env);

            // 设置运行模式
            env.setParallelism(5);

            // 加载数据源
            List<String> listSource = getInitData();
            DataSet<String> elementsSource = env.fromCollection(listSource);


            Set<String> expectedNature = new HashSet<String>() {{
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

            Set<String> stopWords = new HashSet<>();


            // 第一步，对String进行分词，得到Tuple2<String,String>> ("词","词性")
            DataSet<Tuple2<String, String>> splitOperator = elementsSource.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                    //用空格分隔为单词
                    String[] sentenceArr = value.split(" ");
                    //统计单词使用频次，放入收集器
                    for (String sentence : sentenceArr) {
                        Result result = ToAnalysis.parse(sentence); //分词结果的一个封装，主要是一个Term>的terms
                        for (Term term : result.getTerms()) {
                            String word = term.getName(); //拿到词
                            String natureStr = term.getNatureStr(); //拿到词性
                            out.collect(new Tuple2<>(word, natureStr));
                        }
                    }
                }
            });
            //第二步，对Tuple2<String,String>>进行过滤，得到Tuple2<String,String>> ("词","词性")
            //过滤掉词性不是名词的词,和停用词
            //用 Filter
            DataSet<Tuple2<String, String>> filterOperator = splitOperator.filter(new FilterFunction<Tuple2<String, String>>() {
                @Override
                public boolean filter(Tuple2<String, String> value) throws Exception {
                    //过滤掉词性不是名词的词,和停用词
                    boolean flag = expectedNature.contains(value.f1) && !stopWords.contains(value.f0);
                    return flag;
                }
            });


            //第三步，对Tuple2<String,String>>进行计算，得到Tuple2<String,Long>> ("词","词频")
            DataSet<Tuple2<String, Long>> countOperator = filterOperator.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
                    //对Tuple2<String,String>>进行计算，得到Tuple2<String,Long>> ("词","词频")
                    String word = value.f0;
                    Long count = wordCountMap.get(word);
                    if (count == null) {
                        count = 0L;
                    }
                    count++;
                    wordCountMap.put(word, count);
                    return new Tuple2<>(word, count);
                }
            });

            //第四步，去重, 根据词去重
            DataSet<Tuple2<String, Long>> distinctOperator = countOperator.distinct(0);

            //第五步，对Tuple2<String,Long>>进行排序，得到Tuple2<String,Long>> ("词","词频")，根据词频排序
            DataSet<Tuple2<String, Long>> sortedOperator = distinctOperator.sortPartition(1, org.apache.flink.api.common.operators.Order.DESCENDING);

            //第六步，计算全局的IDF词频，得到Map<String,Integer> ("词","IDF")
            //initSet是全局的IDF词频
            //合并，按照二元组第一个字段word分组，把第二个字段统计出来
            DataSet<Tuple2<String, Long>> unionOperator = initSet.union(sortedOperator).groupBy(0).sum(1);


            //输出当前词频
            sortedOperator.print();
            //输出全局词频
            unionOperator.print();

            // 4.输出结果, initSet 输出到 idf.txt中, sortedOperator 输出到 tf.txt中
            initSet.writeAsText("idf.txt",  FileSystem.WriteMode.OVERWRITE);
            sortedOperator.writeAsText("tf.txt",  FileSystem.WriteMode.OVERWRITE);


            // 3.执行
            env.execute("flink-hello-world");

        }
    }
}