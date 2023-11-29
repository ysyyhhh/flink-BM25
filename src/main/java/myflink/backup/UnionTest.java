package myflink.backup;


import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Chris Chan
 * Create on 2021/5/22 7:23
 * Use for:
 * Explain: Flink流式处理从Kafka获取的数据并写入ElasticSearch
 */
public class UnionTest {
    //ElasticSearch内索引名称
    public static final String INDEX_NAME = "topic_flink";
    //本地缓存旧数据
    private static ConcurrentHashMap<String, Long> wordCountMap = new ConcurrentHashMap<>(16);
    //结巴分词器


    public static void main(String[] args) throws Exception {
        new UnionTest().execute(args);
    }

    /**
     * 初始化 缓存ElasticSearch旧数据
     *
     * @param env
     * @return
     */
    private DataStreamSource<Tuple2<String, Long>> init(StreamExecutionEnvironment env) {
        List<Tuple2<String, Long>> wordCountList = new ArrayList<>(wordCountMap.size());
        wordCountMap.forEach((key, value) -> wordCountList.add(new Tuple2<>(key, value)));
        //避免集合为空
        if (wordCountList.size() == 0) {
            wordCountList.add(new Tuple2<>("flink", 0L));
        }
        return env.fromCollection(wordCountList);
    }

    private void execute(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //配置kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "flink.chris.com:9092");
        properties.put("group.id", "flink_group_1");
        //初始化
        DataStreamSource<Tuple2<String, Long>> initStream = init(env);
        //从socket获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9000);
        //wordcount计算
        SingleOutputStreamOperator<Tuple2<String, Long>> operator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            /**
             * map计算
             * @param value 输入数据 用空格分隔的句子
             * @param out map计算之后的收集器
             * @throws Exception
             */
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                //用空格分隔为单词
                String[] words = value.split(" ");
                //统计单词使用频次，放入收集器
                for (String sentence : words) {
                    Result result = ToAnalysis.parse(sentence); //分词结果的一个封装，主要是一个List<Term>的terms
                    List<Tuple2<String,String>> list = new ArrayList<>();
                    for (Term term : result.getTerms()) {
                        String word = term.getName(); //拿到词
                        String natureStr = term.getNatureStr(); //拿到词性
                        out.collect(new Tuple2<>(word, 1L));
                    }
                }
            }
        });
        //合并初始化流，按照二元组第一个字段word分组，把第二个字段统计出来
        SingleOutputStreamOperator<Tuple2<String, Long>> resultOperator = operator.union(initStream).keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        }).sum(1);

        resultOperator.print();


        env.execute();
    }


}