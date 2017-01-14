package com.yuan.document;

import org.apache.lucene.document.Document;

import java.util.List;

/**
 * Created by yuan on 1/9/17.
 */
public abstract class AbstractDoc2ObjectMapper implements Doc2ObjectMapper {
    @Override
    public Object mapDocumentsToObject(List<Document> documents) {
        return null;
    }

    @Override
    public Object mapDocumentToObject(Document document) {
        return null;
    }
}
