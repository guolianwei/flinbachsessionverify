package com.example.flink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
/**
 * Flink mysql 应用
 * */
public class FlinkMySQLJob {
    protected final static Logger  LOGGER = LoggerFactory.getLogger(FlinkMySQLJob.class);
    private static final String MYSQL_URL = "jdbc:mysql://192.168.153.130:3306/hive";
    private static final String MYSQL_USER = "hive";
    private static final String MYSQL_PASSWORD = "Hive@1234";

    public static void main(String[] args) throws Exception {
        // 创建 Flink 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sign="_"+args[0];
        // 设置 MySQL 数据库，初始化输入和输出表
        setupDatabase(sign);

        // 注册 MySQL 输入表
        tableEnv.executeSql(
                "CREATE TABLE InputTable"+sign+" (" +
                        " id INT," +
                        " category STRING," +
                        " `value` DOUBLE" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = '" + MYSQL_URL + "'," +
                        "'table-name' = 'input_table"+sign+"'," +
                        "'username' = '" + MYSQL_USER + "'," +
                        "'password' = '" + MYSQL_PASSWORD + "'" +
                        ")"
        );

        Table inputTableTest = tableEnv.from("InputTable"+sign);

// 打印输入表内容
        DataStream<Row> inputDataStream = tableEnv.toDataStream(inputTableTest);
        inputDataStream.map(row -> {
            System.out.println("输入记录: " + row);
            return row;
        }).print();


        // 注册 MySQL 输出表
        tableEnv.executeSql(
                "CREATE TABLE OutputTable"+sign+" (" +
                        " category STRING," +
                        " sum_value DOUBLE," +
                        " avg_value DOUBLE," +
                        " PRIMARY KEY (category) NOT ENFORCED" + // 添加主键
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = '" + MYSQL_URL + "'," +
                        "'table-name' = 'output_table"+sign+"'," +
                        "'username' = '" + MYSQL_USER + "'," +
                        "'password' = '" + MYSQL_PASSWORD + "'" +
                        ")"
        );



        // 从 InputTable 中读取数据
        Table inputTable = tableEnv.from("InputTable"+sign);

        // 执行统计计算：分组求和和平均
        Table resultTable = inputTable
                .groupBy(Expressions.$("category"))
                .select(
                        Expressions.$("category"),
                        Expressions.$("value").sum().as("sum_value"),
                        Expressions.$("value").avg().as("avg_value")
                );

        // 将结果写入 OutputTable
        resultTable.executeInsert("OutputTable"+sign);

        // 添加查询输出表并打印的过程
        queryAndPrintOutputTable(sign);

        // 删除数据库中的临时表
        //cleanupDatabase();

        LOGGER.info("任务已完成！");
    }

    // 初始化数据库，创建表和样例数据
    private static void setupDatabase(String sign) throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // 创建输入表
            stmt.execute("CREATE TABLE IF NOT EXISTS input_table"+sign+" (" +
                    " id INT AUTO_INCREMENT PRIMARY KEY," +
                    " category VARCHAR(50)," +
                    " value DOUBLE)");

            // 插入样例数据
            for (int i = 1; i <= 1000; i++) {
                String category = "Category_" + (i % 10);
                double value = Math.random() * 100;
                stmt.execute(String.format("INSERT INTO input_table"+sign
                        +" (category, value) VALUES ('%s', %f)", category, value));
            }

            // 创建输出表
            stmt.execute("CREATE TABLE IF NOT EXISTS output_table"+sign+" (" +
                    " category VARCHAR(50) PRIMARY KEY," +
                    " sum_value DOUBLE," +
                    " avg_value DOUBLE)");
        } catch (Exception e) {
            throw new RuntimeException("数据库初始化失败", e);
        }
    }

    // 删除数据库中的临时表
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
            for (int i=0;i<10;i++){
                stmt.execute("DROP TABLE IF EXISTS output_table_"+i);
            }
        } catch (Exception e) {
            throw new RuntimeException("清理数据库失败", e);
        }
    }

    // 查询输出表并打印结果
    private static void queryAndPrintOutputTable(String sign) throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // 查询输出表数据
            ResultSet resultSet = stmt.executeQuery("SELECT category, sum_value, avg_value FROM output_table"+sign);

            // 打印查询结果
            System.out.println("输出表数据：");
            while (resultSet.next()) {
                String category = resultSet.getString("category");
                double sumValue = resultSet.getDouble("sum_value");
                double avgValue = resultSet.getDouble("avg_value");
                System.out.printf("Category: %s, Sum: %.2f, Avg: %.2f%n", category, sumValue, avgValue);
            }
        } catch (Exception e) {
            throw new RuntimeException("查询输出表失败", e);
        }
    }
}
