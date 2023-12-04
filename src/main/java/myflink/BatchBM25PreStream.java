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
//        add("nr");
        add("ns");
        add("nt");
        add("nz");
//        add("nw");
//        add("nl");
//        add("ng");
//                add("t");
//                add("tg");
//                add("s");
//                add("f");
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
        TF tf = null;
        try {
            tf = new TF(0,idfList);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MongoCollection<Document> coll = mongoUtil.getCollection("candidate","idf3");
        //如果已经存在,则更新
        Document doc = tf.toDoc();

        BasicDBObject searchDoc = new BasicDBObject().append("_id",tf.pid);
        BasicDBObject newDoc = new BasicDBObject().append("$set",doc);
        coll.findOneAndUpdate(searchDoc, newDoc, new FindOneAndUpdateOptions().upsert(true));

    }
    DataSet<Tuple2<String, Long> > IDFSet = null;
    DataSet<ConcurrentHashMap<String, Long>> unionSet = null;
    DataSet<ArrayList<Tuple2<String, Long>>> idfList = null;
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
                DataSet<String> elementsSource = env.fromElements(s);


                // 第二步，对String进行分词，得到ArrayList, Tuple2<String,String>> ("词","词性")
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

                //第三步，对ArrayList<Tuple2<String, String>>进行去重，得到ConcurrentHashMap<String, String> ("词","词性")
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

                //第四步，对ConcurrentHashMap<String, String>进行排序，得到ArrayList<Tuple2<String, String>> ("词","词性")
                DataSet<ArrayList<Tuple2<String, Long>> > sortedOperator = distinctOperator.map(new MapFunction<ConcurrentHashMap<String, Long>, ArrayList<Tuple2<String, Long>>>() {
                    @Override
                    public ArrayList<Tuple2<String, Long>> map(ConcurrentHashMap<String, Long> value) throws Exception {
                        ArrayList<Tuple2<String, Long>> wordCountArrayList = new ArrayList<>();


//                        value.forEach((key, value1) -> wordCountArrayList.add(new Tuple2<>(key, value1)));
                        for (String key : value.keySet()) {
                            wordCountArrayList.add(new Tuple2<>(key, value.get(key)));
                        }
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

                // 第五步，保存到MongoDB
                sortedOperator.output(
                        new MongoTFSink(pid)
                );

                env.execute("flink-hello-world");
                // list转换成map
//                DataSet<ConcurrentHashMap<String, Long>> preUnion = sortedOperator.map(new MapFunction<ArrayList<Tuple2<String, Long>>, ConcurrentHashMap<String, Long>>() {
//                    @Override
//                    public ConcurrentHashMap<String, Long> map(ArrayList<Tuple2<String, Long>> value) throws Exception {
//                        ConcurrentHashMap<String, Long> wordMap = new ConcurrentHashMap<>(16);
//                        for (Tuple2<String, Long> tuple2 : value) {
//                            String word = tuple2.f0;
//                            wordMap.put(word, 1L);
//                        }
//                        return wordMap;
//                    }
//                });
//
//
//                // 把preUnion和unionSet合并，按照二元组第一个字段word分组，把第二个字段统计出来
//                unionSet = preUnion.union(unionSet).reduce((value1, value2) -> {
//                    ConcurrentHashMap<String, Long> wordMap = new ConcurrentHashMap<>(16);
//                    for (String key : value1.keySet()) {
//                        if (wordMap.containsKey(key)) {
//                            wordMap.put(key, wordMap.get(key) + 1);
//                        } else {
//                            wordMap.put(key, 1L);
//                        }
//                    }
//                    for (String key : value2.keySet()) {
//                        if (wordMap.containsKey(key)) {
//                            wordMap.put(key, wordMap.get(key) + 1);
//                        } else {
//                            wordMap.put(key, 1L);
//                        }
//                    }
//                    return wordMap;
//                });
//                if(i % 50 == 0)
//                    System.out.println("IDFSet.count() = " + IDFSet.count());

            }
//            unionSet.print();

            System.out.println("unionSet.count() = " + unionSet.count());

            //取unionSet的第一个元素 并转换成List
//            DataSet<ConcurrentHashMap<String, Long>> idfSet = unionSet.first(1);

            //把ConcurrentHashMap<String, Long>转换成List<Tuple2<String, Long>>
//            idfList = unionSet.map(new MapFunction<ConcurrentHashMap<String, Long>, ArrayList<Tuple2<String, Long>>>() {
//                @Override
//                public ArrayList<Tuple2<String, Long>> map(ConcurrentHashMap<String, Long> value) throws Exception {
//                    ArrayList<Tuple2<String, Long>> wordCountArrayList = new ArrayList<>();
//                    for (String key : value.keySet()) {
//                        wordCountArrayList.add(new Tuple2<>(key, value.get(key)));
//                    }
//                    //根据词频排序
//                    wordCountArrayList.sort((o1, o2) -> {
//                        if (o1.f1 > o2.f1) {
//                            return -1;
//                        } else if (o1.f1 < o2.f1) {
//                            return 1;
//                        } else {
//                            return 0;
//                        }
//                    });
//                    return wordCountArrayList;
//                }
//            });

            //保存到MongoDB
//            idfList.output(
//                    new MongoIDFSink()
//            );
            // 把IDFSet的所有元素转为一个list
//            List<Tuple2<String, Long>> idfList = IDFSet.collect();
//            System.out.println("idfList.size() = " + idfList.size());

            // 保存到MongoDB
//            saveIDF(idfList);

//            IDFSet.print();
            env.execute("flink-hello-world");
        }
    }
}