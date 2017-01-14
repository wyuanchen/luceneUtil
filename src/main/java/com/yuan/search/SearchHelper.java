package com.yuan.search;

import com.yuan.document.Doc2ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yuan on 1/8/17.
 */
public class SearchHelper {

    private Analyzer analyzer;
    private String indexDirUrl;
    private Directory directory;
    private IndexReader reader;
    private IndexSearcher indexSearcher;


    public SearchHelper(String indexDirUrl,Analyzer analyzer){
        this.indexDirUrl=indexDirUrl;
        this.analyzer=analyzer;
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SearchHelper(String indexDirUrl){
        this(indexDirUrl, new SmartChineseAnalyzer());
    }


    private void init() throws IOException {
        directory=FSDirectory.open(Paths.get(indexDirUrl));
        reader= DirectoryReader.open(directory);
        indexSearcher=new IndexSearcher(reader);
    }

    /**
     * 查询并且返回经过映射后的对象List
     * @param query
     * @param offset
     * @param topN
     * @return
     * @throws IOException
     */
    public List<Object> search(Query query,int offset,int topN,Sort sort,Doc2ObjectMapper doc2ObjectMapper) throws IOException {
        TopDocs topDocs=null;
        ScoreDoc after=null;
        if(offset>0){
            TopDocs docsBefore=indexSearcher.search(query,offset,sort);
            ScoreDoc[] scoreDocs=docsBefore.scoreDocs;
            if(scoreDocs.length>0)
                after=scoreDocs[scoreDocs.length-1];
        }
        topDocs=indexSearcher.searchAfter(after,query,topN,sort);
        return creatObjectList(topDocs.scoreDocs,doc2ObjectMapper);
    }

    /**
     * 没有Sort的search
     * @param query
     * @param offset
     * @param topN
     * @return
     * @throws IOException
     */
    public List<Object> search(Query query,int offset,int topN,Doc2ObjectMapper doc2ObjectMapper) throws IOException {
        TopDocs topDocs=null;
        ScoreDoc after=null;
        if(offset>0){
            TopDocs docsBefore=indexSearcher.search(query,offset);
            ScoreDoc[] scoreDocs=docsBefore.scoreDocs;
            if(scoreDocs.length>0)
                after=scoreDocs[scoreDocs.length-1];
        }
        topDocs=indexSearcher.searchAfter(after,query,topN);

        return creatObjectList(topDocs.scoreDocs,doc2ObjectMapper);
    }



    /**
     * 获取查询到的总数量
     * @param query
     * @return
     * @throws IOException
     */
    public int getSum(Query query) throws IOException {
        return indexSearcher.search(query,1).totalHits;
    }

    private List<Object> creatObjectList(ScoreDoc[] scoreDocs,Doc2ObjectMapper doc2ObjectMapper) throws IOException {
        List<Object> result=new LinkedList<Object>();
        for(ScoreDoc scoreDoc:scoreDocs){
            result.add(doc2ObjectMapper.mapDocumentToObject(indexSearcher.doc(scoreDoc.doc)));
        }
        return result;
    }

}
