package com.yuan.document;

import org.apache.lucene.document.Document;

import java.util.List;

/**
 * Created by yuan on 1/8/17.
 */
public interface Doc2ObjectMapper {
    /**
     * 将多个Document映射成一个对象
     * @param documents
     * @return
     */
    Object mapDocumentsToObject(List<Document> documents);


    /**
     * 将单个Document映射成一个对象
     * @param document
     * @return
     */
    Object mapDocumentToObject(Document document);
}
