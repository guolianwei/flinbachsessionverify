package com.example.flink.other;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class FlinkAPIUtils {
    /**
     * 【已修改】
     * 检查 JAR 文件是否已存在，如果不存在，则上传它。
     *
     * @param jarFilePath 本地 JAR 文件路径
     * @return 上传或找到的 JAR 文件 ID
     * @throws IOException 发生 I/O 错误时抛出异常
     */
    public static String uploadJar(String jarFilePath,String flinkRestUrl) throws IOException {
        File jarFile = new File(jarFilePath);
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file not found: " + jarFilePath);
        }
        String jarFilename = jarFile.getName();

        // --- 1. 检查 JAR 是否已经存在 ---
        String existingJarId = findJarIdByName(jarFilename,flinkRestUrl);
        if (existingJarId != null) {
            System.out.println("JAR file '" + jarFilename + "' already exists on cluster.");
            return existingJarId; // 找到了，直接返回 ID
        }

        // --- 2. 如果不存在，执行上传 ---
        System.out.println("JAR file '" + jarFilename + "' not found. Uploading...");
        URL url = new URL(flinkRestUrl + "/jars/upload");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        String boundary = "---" + System.currentTimeMillis() + "---"; // 使用动态 boundary
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        // 写入文件内容
        try (OutputStream outputStream = connection.getOutputStream();
             FileInputStream fileInputStream = new FileInputStream(jarFile)) {
            String prefix = "--" + boundary + "\r\n" +
                    "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jarFile.getName() + "\"\r\n" +
                    "Content-Type: application/java-archive\r\n\r\n";
            outputStream.write(prefix.getBytes());

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            String suffix = "\r\n--" + boundary + "--\r\n";
            outputStream.write(suffix.getBytes());
        }

        // 获取响应
        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            // 打印错误详情
            String errorResponse = readResponse(connection, true);
            throw new IOException("Failed to upload JAR file. HTTP code: " + responseCode + ", Response: " + errorResponse);
        }

        // 解析响应
        String responseBody = readResponse(connection, false);

        // 【修复】更健壮的 JSON 解析
        // 响应格式: {"filename":"/tmp/flink-web-..../uuid_filename.jar","status":"success"}
        String fileKey = "\"filename\":\"";
        int startIndex = responseBody.indexOf(fileKey);
        if (startIndex == -1) {
            throw new IOException("Upload response missing 'filename' field: " + responseBody);
        }
        startIndex += fileKey.length();

        int endIndex = responseBody.indexOf('"', startIndex); // 结束符是 "
        if (endIndex == -1) {
            throw new IOException("Upload response has malformed 'filename' field: " + responseBody);
        }

        String fullPath = responseBody.substring(startIndex, endIndex);
        // Flink REST API 使用上传后路径的 basename 作为 jarId
        return fullPath.replaceAll(".*/", "");
    }

    /**
     * 【新增】
     * 通过 Flink REST API (GET /jars) 查找已上传的 JAR。
     *
     * @param jarFilename 要查找的文件名 (例如: my-job-1.0.jar)
     * @return 找到的 JAR ID (例如: d9501a3b-...._my-job-1.0.jar)，如果未找到则返回 null
     * @throws IOException 发生 I/O 错误
     */
    public static String findJarIdByName(String jarFilename,String flinkRestUrl) throws IOException {
        URL url = new URL(flinkRestUrl + "/jars");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            System.err.println("Failed to get /jars list. HTTP code: " + responseCode);
            return null; // 无法获取列表，假设未找到
        }

        String responseBody = readResponse(connection, false);

        // 响应格式: {"address":"...","files":[{"id":"...","name":"...","uploaded":...}, ...]}
        // 我们需要一种不依赖库的方式来解析它

        // 1. 找到 "files" 数组
        String filesKey = "\"files\":[";
        int filesStartIndex = responseBody.indexOf(filesKey);
        if (filesStartIndex == -1) {
            return null; // 响应中没有 "files" 数组
        }
        filesStartIndex += filesKey.length();

        // 2. 找到 "files" 数组的结尾
        int filesEndIndex = responseBody.indexOf(']', filesStartIndex);
        if (filesEndIndex == -1) {
            return null; // 响应格式错误
        }

        // 3. 获取数组内容
        String filesArrayContent = responseBody.substring(filesStartIndex, filesEndIndex);
        if (filesArrayContent.isEmpty()) {
            return null; // "files" 数组为空
        }

        // 4. 按对象分隔
        String[] jarObjects = filesArrayContent.split("\\},");

        String nameNeedle = "\"name\":\"" + jarFilename + "\"";
        String idNeedle = "\"id\":\"";

        for (String jarObject : jarObjects) {
            // 5. 检查对象中是否包含我们的文件名
            if (jarObject.contains(nameNeedle)) {
                // 6. 如果包含，提取其 "id"
                int idIndex = jarObject.indexOf(idNeedle);
                if (idIndex != -1) {
                    idIndex += idNeedle.length();
                    int idEndIndex = jarObject.indexOf('"', idIndex);
                    if (idEndIndex != -1) {
                        return jarObject.substring(idIndex, idEndIndex); // 找到 ID!
                    }
                }
            }
        }

        return null; // 遍历完毕，未找到
    }
    /**
     * 【新增】
     * 从 HttpURLConnection 读取响应流 (InputStream 或 ErrorStream)。
     *
     * @param connection 连接
     * @param isError    是否读取错误流
     * @return 响应体字符串
     * @throws IOException
     */
    public static String readResponse(HttpURLConnection connection, boolean isError) throws IOException {
        InputStream inputStream = null;
        try {
            if (isError) {
                inputStream = connection.getErrorStream();
            } else {
                inputStream = connection.getInputStream();
            }

            // 如果两个流都为 null (例如 200 响应但没有 body，或错误响应也没有 body)
            if (inputStream == null) {
                return "";
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                StringBuilder response = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                return response.toString();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}
