package com.yuan.search;

import com.yuan.document.Doc2ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 使用Group by进行搜索
 * Created by yuan on 1/8/17.
 */
public class GroupSearcherHelper {

    private Analyzer analyzer;
    private String indexDirUrl;
    private Directory directory;
    private IndexReader reader;
    private IndexSearcher indexSearcher;
    private double maxCacheRAMMB;
    private boolean isCacheScores=true;
    private boolean ifFillFields=true;

    public static final double DEFAULT_MAX_CACHE_RAM_MB=4.0;

    public GroupSearcherHelper(String indexDirUrl,Analyzer analyzer,double maxCacheRAMMB){
        this.indexDirUrl=indexDirUrl;
        this.analyzer=analyzer;
        this.maxCacheRAMMB=maxCacheRAMMB;
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public GroupSearcherHelper(String indexDirUrl,Analyzer analyzer){
        this(indexDirUrl,analyzer,DEFAULT_MAX_CACHE_RAM_MB);
    }

    public GroupSearcherHelper(String indexDirUrl){
        this(indexDirUrl,new SmartChineseAnalyzer());
    }



    private void init() throws IOException {
        directory= FSDirectory.open(Paths.get(indexDirUrl));
        reader= DirectoryReader.open(directory);
        indexSearcher=new IndexSearcher(reader);
    }


    /**
     * 搜索返回文档分组
     * @param query
     * @param groupFieldName
     * @param groupSort
     * @param withinGroupSort
     * @param groupOffset
     * @param topNGroups
     * @return
     * @throws IOException
     */
    public List<List<Document>> searchDocument(Query query, String groupFieldName, Sort groupSort, Sort withinGroupSort, int groupOffset, int topNGroups) throws IOException {
        List<List<Document>> result=new LinkedList<List<Document>>();
        TopGroups<BytesRef> topGroupsResult=searchHelp(query,groupFieldName,groupSort,withinGroupSort,groupOffset,topNGroups);
        if(topGroupsResult==null)
            return result;
        GroupDocs<BytesRef>[] groupDocses=topGroupsResult.groups;
        for(GroupDocs<BytesRef> groupDocs:groupDocses){
            List<Document> subList=new LinkedList<Document>();
            for(ScoreDoc scoreDoc:groupDocs.scoreDocs){
                Document document=indexSearcher.doc(scoreDoc.doc);
                subList.add(document);
            }
            result.add(subList);
        }

        return result;
    }

    /**
     * 使用默认Sort的searchDocument
     * @param query
     * @param groupFieldName
     * @param groupOffset
     * @param topNGroups
     * @return
     * @throws IOException
     */
    public List<List<Document>> searchDocument(Query query, String groupFieldName, int groupOffset, int topNGroups) throws IOException {
        return searchDocument(query,groupFieldName,Sort.INDEXORDER,Sort.INDEXORDER,groupOffset,topNGroups);
    }

    /**
     * 分组搜索并且将每一组Document映射成一个对象并且返回所有对象组成的List
     * @param query
     * @param groupFieldName
     * @param groupSort
     * @param withinGroupSort
     * @param groupOffset
     * @param topNGroups
     * @param mapper
     * @return
     * @throws IOException
     */
    public List<Object> search(Query query, String groupFieldName, Sort groupSort, Sort withinGroupSort, int groupOffset, int topNGroups, Doc2ObjectMapper mapper) throws IOException {
        List<Object> result=new LinkedList<Object>();
        List<List<Document>> documentsList=searchDocument(query,groupFieldName,groupSort,withinGroupSort,groupOffset,topNGroups);
        if(documentsList.size()==0)
            return result;
        Object o=null;
        for(List<Document> documents:documentsList){
            o=mapper.mapDocumentsToObject(documents);
            result.add(o);
        }
        return result;
    }

    /**
     * 使用默认Sort的search
     * @param query
     * @param groupFieldName
     * @param groupOffset
     * @param topNGroups
     * @param mapper
     * @return
     * @throws IOException
     */
    public List<Object> search(Query query, String groupFieldName,  int groupOffset, int topNGroups, Doc2ObjectMapper mapper) throws IOException {
        return search(query,groupFieldName,Sort.INDEXORDER,Sort.INDEXORDER,groupOffset,topNGroups,mapper);
    }



    TopGroups<BytesRef> searchHelp(Query query, String groupFieldName, Sort groupSort, Sort withinGroupSort, int groupOffset, int topNGroups) throws IOException {
        TermFirstPassGroupingCollector c1=new TermFirstPassGroupingCollector(groupFieldName,groupSort,groupOffset+topNGroups);
        /**
         * 将TermFirstPassGroupingCollector包装成CachingCollector，为第一次查询加缓存，避免重复评分
         *  CachingCollector就是用来为结果收集器添加缓存功能的
         */
        CachingCollector cachingCollector=CachingCollector.create(c1,isCacheScores,maxCacheRAMMB);
        //开始第一次分组统计
        indexSearcher.search(query,cachingCollector);

        /**第一次查询返回的结果集TopGroups中只有分组域值以及每组总的评分，至于每个分组里有几条，分别哪些索引文档，则需要进行第二次查询获取*/
        Collection<SearchGroup<BytesRef>> topGroups=c1.getTopGroups(groupOffset,ifFillFields);
        if(topGroups==null){
            return null;
        }

        Collector secondPassCollector=null;
        // 是否获取每个分组内部每个索引的评分
        boolean ifGetScores=true;
        // 是否计算最大评分
        boolean ifGetMaxScores=true;
        int maxDocsPerGroup=10;
        // 如果需要对Lucene的score进行修正，则需要重载TermSecondPassGroupingCollector
        TermSecondPassGroupingCollector c2=new TermSecondPassGroupingCollector(groupFieldName,topGroups,
                groupSort,withinGroupSort,
                maxDocsPerGroup,ifGetScores,ifGetMaxScores,ifFillFields);

//        // 如果需要计算总的分组数量，则需要把TermSecondPassGroupingCollector包装成TermAllGroupsCollector
//        // TermAllGroupsCollector就是用来收集总分组数量的
//        TermAllGroupsCollector allGroupsCollector = null;
//        //若需要统计总的分组数量
//        if (requiredTotalGroupCount) {
//            allGroupsCollector = new TermAllGroupsCollector("author");
//            secondPassCollector = MultiCollector.wrap(c2, allGroupsCollector);
//        } else {
//            secondPassCollector = c2;
//        }

        secondPassCollector=c2;

        /**如果第一次查询已经加了缓存，则直接从缓存中取*/
        if(cachingCollector.isCached()){
            //第二次查询直接从缓存中取
            cachingCollector.replay(secondPassCollector);
        }else{
            // 开始第二次分组查询
            indexSearcher.search(query,secondPassCollector);
        }


//        /** 所有满足条件的记录数 */
//        int totalHitCount = 0;
//        /** 所有组内的满足条件的记录数(通常该值与totalHitCount是一致的) */
//        int totalGroupedHitCount = -1;

        TopGroups<BytesRef> topGroupsResult=c2.getTopGroups(0);

//        totalHitCount=topGroupsResult.totalHitCount;
//        totalGroupedHitCount=topGroupsResult.totalGroupedHitCount;
//        System.out.println("groupsResult.totalHitCount:" + totalHitCount);
//        System.out.println("groupsResult.totalGroupedHitCount:"
//                + totalGroupedHitCount);
//        System.out.println();
        return topGroupsResult;
    }

    /**
     * 查询符合条件的分组总数量
     * @param query
     * @param groupFieldName
     * @return
     * @throws Exception
     */
    public int getGroupSum(Query query,String groupFieldName) throws Exception{
        TermFirstPassGroupingCollector c1=new TermFirstPassGroupingCollector(groupFieldName,Sort.INDEXORDER,1);
        TermAllGroupsCollector termAllGroupsCollector=new TermAllGroupsCollector(groupFieldName);
        Collector collector=  MultiCollector.wrap(c1,termAllGroupsCollector);
        indexSearcher.search(query,collector);

        return termAllGroupsCollector.getGroupCount();
    }

//    public List<List<Document>> search(Query query, String groupFieldName, Sort sort, int page, int pageSize) throws IOException {
//        GroupingSearch groupingSearch=new GroupingSearch(groupFieldName);
//        groupingSearch.setGroupSort(sort);
//        groupingSearch.setFillSortFields(true);
//        groupingSearch.setCachingInMB(maxCacheRAMMB,true);
//        groupingSearch.setAllGroups(true);
//        TopGroups<BytesRef> topGroups=groupingSearch.search(indexSearcher,query,(page-1)*pageSize,page*pageSize);
//
//        GroupDocs<BytesRef>[] groupDocses=topGroups.groups;
//        List<List<Document>> result=new LinkedList<List<Document>>();
//        for(GroupDocs<BytesRef> groupDocs:groupDocses){
//            List<Document> subList=new LinkedList<Document>();
//            for(ScoreDoc scoreDoc:groupDocs.scoreDocs){
//                Document document=indexSearcher.doc(scoreDoc.doc);
//                subList.add(document);
//            }
//            result.add(subList);
//        }
//
//        return result;
//    }
}
