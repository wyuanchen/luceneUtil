package com.yuan.build;

import com.yuan.document.DocumentCreator;
import com.yuan.util.DBUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by yuan on 1/7/17.
 */
public class IndexBuilder {
    private String url;
    private String user;
    private String password;
    private String sql;
    private DBUtil dbUtil;
    private Analyzer analyzer=new SmartChineseAnalyzer();
    private String indexDirUrl ;
    private DocumentCreator documentCreator;

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void setDbUtil(DBUtil dbUtil) {
        this.dbUtil = dbUtil;
    }

    public void setDocumentCreator(DocumentCreator documentCreator) {
        this.documentCreator = documentCreator;
    }

    public void setIndexDirUrl(String indexDirUrl) {
        this.indexDirUrl = indexDirUrl;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public IndexBuilder(){}

    /**
     * 从数据库查询获取结果集
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public ResultSet getResultSet() throws ClassNotFoundException, SQLException {
        dbUtil=new DBUtil(url,user,password);
        Connection conn=dbUtil.getConnection();
        Statement statement=conn.createStatement();
        return statement.executeQuery(sql);
    }

    /**
     * 结束工作
     */
    public void complete(){
        if(dbUtil!=null)
            dbUtil.closeConnection();
        dbUtil=null;
    }

    /**
     * 启动建立索引
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    public void start() throws Exception {
        System.out.println("luceneIndexBuilder start!");
        long startTime=System.currentTimeMillis();

        ResultSet rs=getResultSet();

        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        Directory directory= FSDirectory.open(Paths.get(indexDirUrl));
        IndexWriter indexWriter=new IndexWriter(directory,indexWriterConfig);

        Document doc=null;
        while(rs.next()){
            doc=documentCreator.createDocument(rs);
            indexWriter.addDocument(doc);
        }
        indexWriter.close();
        complete();
        long endTime=System.currentTimeMillis();
        System.out.println("luceneIndexBuilder complete");

    }


}