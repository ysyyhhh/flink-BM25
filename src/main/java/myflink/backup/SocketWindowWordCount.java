package myflink.backup;
import myflink.util.TfIdfUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.domain.Result;
import org.ansj.domain.Term;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class SocketWindowWordCount {

    private static ConcurrentHashMap<String, Long> wordCountMap = new ConcurrentHashMap<>(16);
    private DataStreamSource<List<Tuple2<String, Long>>> init(StreamExecutionEnvironment env) {
        List<Tuple2<String, Long>> wordCountList = new ArrayList<>(wordCountMap.size());
        wordCountMap.forEach((key, value) -> wordCountList.add(new Tuple2<>(key, value)));
        //避免集合为空
        if (wordCountList.size() == 0) {
            wordCountList.add(new Tuple2<>("flink", 0L));
        }
        return env.fromElements(wordCountList);
    }

    public static void main(String[] args) throws Exception {
        new SocketWindowWordCount().execute(args);
    }
    private void execute(String[] args) throws Exception {
        {

            // 1.准备环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStreamSource<List<Tuple2<String, Long>>> initStream = init(env);

            // 设置运行模式
            env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
            // 2.加载数据源
            DataStreamSource<String> elementsSource = env.socketTextStream("localhost", 9000);
            //
            Set<String> expectedNature = new HashSet<String>() {{
                add("n");
                add("v");
                add("vd");
                add("vn");
                add("vf");
                add("vx");
                add("vi");
                add("vl");
                add("vg");
                add("nt");
                add("nz");
                add("nw");
                add("nl");
                add("ng");
                add("userDefine");
                add("wh");
            }};

            Set<String> stopWords = new HashSet<>();

            TfIdfUtil tfIdfUtil = new TfIdfUtil();

    ////                tfIdfUtil.addDocument(value);


            // 第一步，对String进行分词，得到List<Tuple2<String,String>> ("词","词性")
            SingleOutputStreamOperator<List<Tuple2<String,String>> > splitOperator = elementsSource.flatMap(new FlatMapFunction<String, List<Tuple2<String,String>> >() {
                @Override
                public void flatMap(String value, Collector<List<Tuple2<String,String>> > out) throws Exception {
                    //用空格分隔为单词
                    String[] sentenceArr = value.split(" ");
                    //统计单词使用频次，放入收集器
                    for (String sentence : sentenceArr) {
                        Result result = ToAnalysis.parse(sentence); //分词结果的一个封装，主要是一个List<Term>的terms
                        List<Tuple2<String,String>> list = new ArrayList<>();
                        for (Term term : result.getTerms()) {
                            String word = term.getName(); //拿到词
                            String natureStr = term.getNatureStr(); //拿到词性
                            if(expectedNature.contains(natureStr)) {
                                list.add(new Tuple2<>(word, natureStr));
                            }
                        }
                        out.collect(list);
                    }
                }
            });

            //第二步，对List<Tuple2<String,String>>进行过滤，得到List<Tuple2<String,String>> ("词","词性")
            //过滤掉词性不是名词的词,和停用词
            //用 Filter
            SingleOutputStreamOperator<List<Tuple2<String, String> > > filterOperator = splitOperator.filter(new FilterFunction<List<Tuple2<String, String> > >() {
                @Override
                public boolean filter(List<Tuple2<String, String> > value) throws Exception {
                    //过滤掉词性不是名词的词,和停用词
                    boolean flag = true;
                    for (Tuple2<String, String> tuple2 : value) {
                        if(!expectedNature.contains(tuple2.f1)) {
                            flag = false;
                            break;
                        }
                        if(stopWords.contains(tuple2.f0)) {
                            flag = false;
                            break;
                        }
                    }
                    return flag;
                }
            });

            //第三步，对List<Tuple2<String,String>>进行计算，得到List<Tuple2<String,Long>> ("词","词频")
            SingleOutputStreamOperator<List<Tuple2<String, Long >>> countOperator = filterOperator.map(new MapFunction<List<Tuple2<String, String> >, List<Tuple2<String, Long > > >() {
                @Override
                public List<Tuple2<String, Long > > map(List<Tuple2<String, String> > value) throws Exception {
                    //对list进行计算
                    List<Tuple2<String, Long>> list = new ArrayList<>();
                    Map<String,Integer> countMap = new HashMap<String,Integer>();
                    for (Tuple2<String, String> tuple2 : value) {
                        if(countMap.containsKey(tuple2.f0)) {
                            countMap.put(tuple2.f0, countMap.get(tuple2.f0)+1);
                        }else {
                            countMap.put(tuple2.f0, 1);
                        }
                    }
                    countMap.forEach((key, value1) -> list.add(new Tuple2<>(key, value1.longValue())));
                    return list;
                }
            });

            //第四步，对List<Tuple2<String,Long>>进行排序，得到List<Tuple2<String,Long>> ("词","词频")，根据词频排序
            //对list进行排序
            SingleOutputStreamOperator<List<Tuple2<String, Long>>> sortedOperator = countOperator.map(new MapFunction<List<Tuple2<String, Long>>, List<Tuple2<String, Long>>>() {
                @Override
                public List<Tuple2<String, Long>> map(List<Tuple2<String, Long>> value) throws Exception {
                    //对list进行排序
                    value.sort(new Comparator<Tuple2<String, Long>>() {
                        @Override
                        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                            return o2.f1.compareTo(o1.f1);
                        }
                    });
                    return value;
                }
            });


            //第五步，计算全局的IDF词频，得到Map<String,Integer> ("词","IDF")
            //initStream是全局的IDF词频

            //合并，按照二元组第一个字段word分组，把第二个字段统计出来
            SingleOutputStreamOperator<List<Tuple2<String, Long>>> unionOperator = sortedOperator.union(initStream).keyBy(new KeySelector<List<Tuple2<String, Long>>, String>() {
                @Override
                public String getKey(List<Tuple2<String, Long>> value) throws Exception {
                    return value.get(0).f0;
                }
            }).reduce((value1, value2) -> {
                List<Tuple2<String, Long>> list = new ArrayList<>();
                list.addAll(value1);
                list.addAll(value2);
                return list;
            }).map(new MapFunction<List<Tuple2<String, Long>>, List<Tuple2<String, Long>>>() {
                @Override
                public List<Tuple2<String, Long>> map(List<Tuple2<String, Long>> value) throws Exception {
                    //对list进行计算
                    List<Tuple2<String, Long>> list = new ArrayList<>();
                    Map<String,Integer> countMap = new HashMap<String,Integer>();
                    for (Tuple2<String, Long> tuple2 : value) {
                        if(countMap.containsKey(tuple2.f0)) {
                            countMap.put(tuple2.f0, countMap.get(tuple2.f0)+1);
                        }else {
                            countMap.put(tuple2.f0, 1);
                        }
                    }
                    countMap.forEach((key, value1) -> list.add(new Tuple2<>(key, value1.longValue())));
                    return list;
                }
            }).map(new MapFunction<List<Tuple2<String, Long>>, List<Tuple2<String, Long>>>() {
                @Override
                public List<Tuple2<String, Long>> map(List<Tuple2<String, Long>> value) throws Exception {
                    //对list进行排序
                    value.sort(new Comparator<Tuple2<String, Long>>() {
                        @Override
                        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                            return o2.f1.compareTo(o1.f1);
                        }
                    });
                    return value;
                }
            });

            //输出当前词频
            sortedOperator.print();
            //输出全局词频
            unionOperator.print();
//            flatMap.print();
            // 5.执行程序
            env.execute("flink-hello-world");

        }
    }
}