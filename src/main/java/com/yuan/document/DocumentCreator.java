package com.yuan.document;

import org.apache.lucene.document.Document;

import java.sql.ResultSet;

/**
 * Created by yuan on 1/8/17.
 */
public interface DocumentCreator {
    Document createDocument(ResultSet resultSet) throws Exception;
}
