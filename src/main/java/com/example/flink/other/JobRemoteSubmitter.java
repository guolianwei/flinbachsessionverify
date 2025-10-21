package com.example.flink.other;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JobRemoteSubmitter {
    public static void main(String[] args) throws Exception {
        // Flink 集群 JobManager 的 REST 地址和端口
//        String jobManagerHost = "192.168.153.130";
        String jobManagerHost = "nn2";
//        int jobManagerPort = 8081;
        int jobManagerPort = 35317;

        // 本地打包的 JAR 文件路径
        String jarPath =
                "D:\\srcs\\flinbachsessionverify\\flinbachsessionverify\\target\\" +
                        "flinbachsessionverify-1.0-SNAPSHOT.jar";

        // 创建远程执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                jobManagerHost, jobManagerPort, jarPath
        );

        // 创建 Table 环境（如果需要）
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 示例作业：打印 100 个数字
        env.fromSequence(1, 100).map(num -> "Number: " + num).print();

        // 提交任务
        env.execute("Remote Flink Job");
    }
}
