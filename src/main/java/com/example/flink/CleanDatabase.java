package com.example.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CleanDatabase {
    protected final static Logger LOGGER = LoggerFactory.getLogger(FlinkMySQLJob.class);
    private static final String MYSQL_URL = "jdbc:mysql://192.168.153.130:3306/hive";
    private static final String MYSQL_USER = "hive";
    private static final String MYSQL_PASSWORD = "Hive@1234";
/**
 * 清理生成的数据
 * */
    public static void main(String args[]) throws ClassNotFoundException {
        cleanupDatabase();
    }
    private static void cleanupDatabase() throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement stmt = conn.createStatement()) {
            // 删除数据表
            stmt.execute("DROP TABLE IF EXISTS input_table_0");
            stmt.execute("DROP TABLE IF EXISTS input_table_1");
            stmt.execute("DROP TABLE IF EXISTS input_table_2");
            stmt.execute("DROP TABLE IF EXISTS input_table_3");
            stmt.execute("DROP TABLE IF EXISTS input_table_4");
            stmt.execute("DROP TABLE IF EXISTS input_table_5");
            stmt.execute("DROP TABLE IF EXISTS input_table_6");
            stmt.execute("DROP TABLE IF EXISTS input_table_7");
            stmt.execute("DROP TABLE IF EXISTS input_table_8");
            stmt.execute("DROP TABLE IF EXISTS input_table_9");
            for (int i=0;i<40000;i++){
                stmt.execute("DROP TABLE IF EXISTS input_table_"+i);
            }
            for (int i=0;i<40000;i++){
                stmt.execute("DROP TABLE IF EXISTS output_table_"+i);
            }
        } catch (Exception e) {
            throw new RuntimeException("清理数据库失败", e);
        }
    }
}
