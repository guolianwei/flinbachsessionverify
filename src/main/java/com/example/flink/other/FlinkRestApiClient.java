package com.example.flink.other;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class FlinkRestApiClient {

    private static final String FLINK_REST_URL = "http://nn2:58084"; // Flink REST API 地址
    private static final String JAR_FILE_PATH = "D:\\srcs\\flinbachsessionverify\\flinbachsessionverify\\target\\flinbachsessionverify-1.0-SNAPSHOT.jar"; // 本地 JAR 文件路径
    private static final String ENTRY_CLASS = "com.example.flink.FlinkMySQLJob"; // 入口类全名

    public static void main(String[] args) {
        try {
            // 1. 上传 JAR 文件 (现在会先检查是否存在)
            String jarId = FlinkAPIUtils.uploadJar(JAR_FILE_PATH,FLINK_REST_URL);
            if (jarId != null) {
                System.out.println("Using JAR ID: " + jarId);
                Thread.sleep(2000); // 暂停一会，确保 Flink 状态一致
                // 2. 提交任务
                submitJob(jarId, ENTRY_CLASS, "--arg1 value1 --arg2 value2");
            } else {
                System.err.println("Failed to get or upload JAR ID.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    /**
     * 提交任务到 Flink 集群
     * (此函数未修改)
     */
    public static void submitJob(String jarId, String entryClass, String programArgs) throws IOException {
        URL url = new URL(FLINK_REST_URL + "/jars/" + jarId + "/run");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");

        // 构建提交任务的 JSON 数据
        String jsonPayload = "{"
                + "\"entryClass\":\"" + entryClass + "\","
                + "\"parallelism\":3,"
                + "\"programArgs\":\"" + programArgs + "\""
                + "}";

        // 发送请求
        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(jsonPayload.getBytes());
        }

        // 获取响应
        int responseCode = connection.getResponseCode();
        String responseBody = FlinkAPIUtils.readResponse(connection, responseCode != 200);

        if (responseCode != 200) {
            throw new IOException("Failed to submit job. HTTP code: " + responseCode + ", Response: " + responseBody);
        }

        System.out.println("Job submitted successfully. Response:");
        System.out.println(responseBody);
    }


}