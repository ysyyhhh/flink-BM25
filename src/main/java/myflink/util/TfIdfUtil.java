package myflink.util;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TfIdfUtil implements Serializable{
    // 文档表
    private List<String> documents;
    // 文档与词汇列表
    private List<List<String>> documentWords;
    // 文档词频统计表
    private List<Map<String,Integer>> docuementTfList;
    // 词汇出现文档数统计表
    private Map<String,Double>  idfMap;
    // 是否只抽取名词
    private boolean onlyNoun = false;

    public TfIdfUtil(List<String> documents) {
        this.documents = documents;
    }

    public TfIdfUtil(){
        this.documents = new ArrayList<String>();
    }

    public List<Map<String,Double>> eval(){
        this.splitWord();
        this.calTf();
        this.calIdf();
        return calTfIdf();
    }

    /**
     * 获取所有文档数，用于逆文档频率 IDF 的计算
     * @author Shaobin.Ou
     * @return 所有文档数
     */
    private int getDocumentCount() {
        return documents.size();
    }

    public void addDocument(String document) {
        documents.add(document);
    }
    public List<String> splitOneDocument(String oneValue) {
        Result splitWordRes = DicAnalysis.parse(oneValue);
        List<String> wordList = new ArrayList<String>();
        for(Term term : splitWordRes.getTerms()) {
            if(onlyNoun) {
                if(term.getNatureStr().equals("n")||term.getNatureStr().equals("ns")||term.getNatureStr().equals("nz")) {
                    wordList.add(term.getName());
                }
            }else {
                wordList.add(term.getName());
            }
        }
        documentWords.add(wordList);
        return wordList;
    }




    /**
     * 对每一个文档进行词语切分
     * @author Shaobin.Ou
     */
    private void splitWord() {
        documentWords = new ArrayList<List<String>>();
        for( String document : documents ) {
            splitOneDocument(document);
        }
    }

    /**
     * 对每一个文档进行词频的计算
     * @author Shaobin.Ou
     */
    private void calTf() {
        docuementTfList = new ArrayList<Map<String,Integer>>();
        for(List<String> wordList : documentWords ) {
            Map<String,Integer> countMap = new HashMap<String,Integer>();
            for(String word : wordList) {
                if(countMap.containsKey(word)) {
                    countMap.put(word, countMap.get(word)+1);
                }else {
                    countMap.put(word, 1);
                }
            }
            docuementTfList.add(countMap);
        }
    }

    /**
     * 计算逆文档频率 IDF
     * @author Shaobin.Ou
     */
    private void calIdf() {
        int documentCount = getDocumentCount();
        idfMap = new HashMap<String,Double>();
        // 统计词语在多少文档里面出现了
        Map<String,Integer> wordAppearendMap = new HashMap<String,Integer>();
        for(Map<String,Integer> countMap : docuementTfList  ) {
            for(String word : countMap.keySet()) {
                if(wordAppearendMap.containsKey(word)) {
                    wordAppearendMap.put(word, wordAppearendMap.get(word)+1);
                }else {
                    wordAppearendMap.put(word, 1);
                }
            }
        }
        // 对每个词语进行计算
        for(String word : wordAppearendMap.keySet()) {
            double idf = Math.log( documentCount  / (wordAppearendMap.get(word)+1)  );
            idfMap.put(word, idf);
        }
    }

    private List<Map<String,Double>> calTfIdf() {
        List<Map<String,Double>> tfidfRes = new ArrayList<Map<String,Double>>();
        for(Map<String,Integer> docuementTfMap : docuementTfList ) {
            Map<String,Double> tfIdf = new HashMap<String,Double>();
            for(String word : docuementTfMap.keySet()) {
                double tfidf = idfMap.get(word) * docuementTfMap.get(word);
                tfIdf.put(word, tfidf);
            }
            tfidfRes.add(tfIdf);
        }
        return tfidfRes;
    }

}

