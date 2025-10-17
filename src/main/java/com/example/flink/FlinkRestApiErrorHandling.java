package com.example.flink;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class FlinkRestApiErrorHandling {

    private static final String FLINK_REST_URL = "http://192.168.153.130:8081"; // Flink REST API 地址
    private static final String JAR_FILE_PATH = "D:\\srcs\\flinbachsessionverify\\flinbachsessionverify\\target\\flinbachsessionverify-1.0-SNAPSHOT.jar"; // 本地 JAR 文件路径

    public static void main(String[] args) {
        try {
            String jarId = uploadJar(JAR_FILE_PATH);
            System.out.println("Uploaded JAR ID: " + jarId);
            // 示例：提交任务，假设已知 JAR ID
            submitSync(jarId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void submitAsync(String jarId) {
        final  String entryClass = "com.example.flink.FlinkMySQLJob";
        for (int i=0;i<10;i++){
            final  int index=i;
           Thread thread= new Thread(() -> submitJob(jarId, entryClass, ""+index));
            thread.start();
        }
    }
    private static void submitSync(String jarId) throws InterruptedException {
        final  String entryClass = "com.example.flink.FlinkMySQLJob";
        for (int i=0;i<1000000;i++){
            final  int index=i;
            Thread.sleep(100);
            System.out.println("job_"+index);
            submitJob(jarId, entryClass, ""+index);
        }
    }

    /**
     * 上传 JAR 文件到 Flink 集群
     *
     * @param jarFilePath 本地 JAR 文件路径
     * @return 上传成功后的 JAR 文件 ID
     * @throws IOException 上传失败时抛出异常
     */
    public static String uploadJar(String jarFilePath) throws IOException {
        URL url = new URL(FLINK_REST_URL + "/jars/upload");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=---123456");

        // 准备上传文件
        File jarFile = new File(jarFilePath);
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file not found: " + jarFilePath);
        }

        // 写入文件内容
        try (OutputStream outputStream = connection.getOutputStream();
             FileInputStream fileInputStream = new FileInputStream(jarFile)) {
            outputStream.write(("-----123456\r\n" +
                    "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jarFile.getName() + "\"\r\n" +
                    "Content-Type: application/java-archive\r\n\r\n").getBytes());
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.write("\r\n-----123456--\r\n".getBytes());
        }

        // 获取响应
        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new IOException("Failed to upload JAR file. HTTP code: " + responseCode);
        }

        // 解析响应
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            String responseBody = response.toString();
            // 提取 JAR ID
            int startIndex = responseBody.indexOf("\"filename\":\"") + 11;
            int endIndex = responseBody.indexOf("\",", startIndex);
            return responseBody.substring(startIndex, endIndex).replaceAll(".*/", "");
        }
    }

    /**
     * 提交任务到 Flink 集群
     *
     * @param jarId      上传的 JAR 文件 ID
     * @param entryClass 主类的全限定名
     * @param programArgs 程序参数
     */
    public static void submitJob(String jarId, String entryClass, String programArgs) {
        try {
            // 构造提交任务的 URL
            URL url = new URL(FLINK_REST_URL + "/jars/" + jarId + "/run");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");

            // 构建提交任务的 JSON 数据
            String jsonPayload = "{"
                    + "\"entryClass\":\"" + entryClass + "\","
                    + "\"parallelism\":1,"
                    + "\"programArgs\":\"" + programArgs + "\""
                    + "}";

            // 发送请求
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(jsonPayload.getBytes());
            }

            // 获取响应码
            int responseCode = connection.getResponseCode();

            if (responseCode == 200) {
                // 任务提交成功，读取响应
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Response: " + line);
                    }
                }
            } else {
                // 任务提交失败，读取错误信息
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getErrorStream()))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        errorResponse.append(line).append("\n");
                    }
                    System.err.println("Error Response: " + errorResponse.toString());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
