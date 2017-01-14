package com.yuan.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by yuan on 1/6/17.
 */
public class DBUtil {

    private String url;
    private String user;
    private String password;
    private Connection conn;

    public DBUtil(String url,String user,String password) throws ClassNotFoundException {
        this.url=url;
        this.user=user;
        this.password=password;
        Class.forName("com.mysql.jdbc.Driver");
    }

    /**
     * 返回一个数据库连接
     * @return
     */
    public Connection getConnection(){
        try {
            if(conn==null)
                conn= DriverManager.getConnection(url,user,password);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *关闭数据库连接
     */
    public void closeConnection(){
        try {
            if (conn != null) {
                conn.close();
            }
        }catch (SQLException e){
            e.printStackTrace();
        } finally {
          conn=null;
        }
    }
}
