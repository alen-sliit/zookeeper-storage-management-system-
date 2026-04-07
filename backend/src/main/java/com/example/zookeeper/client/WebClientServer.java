package com.example.zookeeper.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Web-based UI server for the distributed storage client.
 * Provides a browser interface for file operations.
 */
public class WebClientServer {
    private static final Logger logger = LoggerFactory.getLogger(WebClientServer.class);
    private final StorageClient storageClient;
    private final HttpServer server;
    private final int port;
    
    public WebClientServer(String zookeeperAddress, int port) throws Exception {
        this.storageClient = new StorageClient(zookeeperAddress);
        this.port = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        
        setupRoutes();
        
        logger.info("Web UI server started at http://localhost:{}", port);
    }
    
    private void setupRoutes() {
        server.createContext("/", new StaticFileHandler("webui"));
        server.createContext("/api/upload", new UploadHandler());
        server.createContext("/api/download", new DownloadHandler());
        server.createContext("/api/delete", new DeleteHandler());
        server.createContext("/api/list", new ListHandler());
        server.createContext("/api/info", new InfoHandler());
        server.createContext("/api/stats", new StatsHandler());
        server.createContext("/api/leader", new LeaderHandler());
        server.createContext("/api/servers", new ServersHandler());
    }
    
    public void start() {
        server.start();
        logger.info("Web UI is ready at http://localhost:{}/", port);
        System.out.println("\n========================================");
        System.out.println("Web UI is running at: http://localhost:" + port);
        System.out.println("Open this URL in your browser");
        System.out.println("Press Ctrl+C to stop");
        System.out.println("========================================\n");
    }
    
    public void stop() {
        server.stop(0);
        try {
            storageClient.close();
        } catch (Exception e) {
            logger.error("Error closing storage client", e);
        }
    }
    
    // API Handlers
    private class UploadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            
            try {
                // Parse multipart form data
                String boundary = getBoundary(exchange);
                Map<String, byte[]> formData = parseMultipartForm(exchange, boundary);

                if (!formData.containsKey("filename") || !formData.containsKey("file")) {
                    String error = "{\"error\": \"Invalid multipart payload\"}";
                    exchange.sendResponseHeaders(400, error.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(error.getBytes());
                    }
                    return;
                }

                String filename = new String(formData.get("filename"), StandardCharsets.UTF_8);
                byte[] content = formData.get("file");
                
                boolean success = storageClient.uploadFile(filename, content);
                
                String response = "{\"success\": " + success + "}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                
            } catch (Exception e) {
                logger.error("Upload error", e);
                String error = "{\"error\": \"" + e.getMessage() + "\"}";
                exchange.sendResponseHeaders(500, error.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(error.getBytes());
                }
            }
        }
    }
    
    private class DownloadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            
            String query = exchange.getRequestURI().getQuery();
            String filename = extractQueryParam(query, "filename");
            
            try {
                byte[] content = storageClient.downloadFile(filename);
                
                if (content != null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().set("Content-Disposition", "attachment; filename=\"" + filename + "\"");
                    exchange.sendResponseHeaders(200, content.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(content);
                    }
                } else {
                    exchange.sendResponseHeaders(404, -1);
                }
                
            } catch (Exception e) {
                logger.error("Download error", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
    
    private class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            
            try {
                String body = new String(readInputStream(exchange.getRequestBody()), StandardCharsets.UTF_8);
                String filename = extractJsonParam(body, "filename");
                
                boolean success = storageClient.deleteFile(filename);
                
                String response = "{\"success\": " + success + "}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                
            } catch (Exception e) {
                logger.error("Delete error", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
    
    private class ListHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                List<StorageClient.FileInfo> files = storageClient.listFiles();
                StringBuilder json = new StringBuilder("[");
                
                for (int i = 0; i < files.size(); i++) {
                    StorageClient.FileInfo file = files.get(i);
                    if (i > 0) json.append(",");
                    json.append("{")
                        .append("\"filename\":\"").append(escapeJson(file.getFilename())).append("\",")
                        .append("\"size\":").append(file.getSize()).append(",")
                        .append("\"modifiedAt\":\"").append(file.getModifiedAt()).append("\"")
                        .append("}");
                }
                json.append("]");
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, json.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(json.toString().getBytes());
                }
                
            } catch (Exception e) {
                logger.error("List error", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
    
    private class InfoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getQuery();
            String filename = extractQueryParam(query, "filename");
            
            try {
                StorageClient.FileMetadata metadata = storageClient.getFileMetadata(filename);
                
                if (metadata != null && !metadata.isDeleted()) {
                    String json = "{"
                        + "\"filename\":\"" + escapeJson(metadata.getFilename()) + "\","
                        + "\"fileId\":\"" + metadata.getFileId() + "\","
                        + "\"size\":" + metadata.getSize() + ","
                        + "\"createdAt\":\"" + metadata.getCreatedAt() + "\","
                        + "\"modifiedAt\":\"" + metadata.getModifiedAt() + "\","
                        + "\"version\":" + metadata.getVersion() + ","
                        + "\"locations\":" + jsonArray(metadata.getLocations())
                        + "}";
                    
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, json.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(json.getBytes());
                    }
                } else {
                    exchange.sendResponseHeaders(404, -1);
                }
                
            } catch (Exception e) {
                logger.error("Info error", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
    
    private class StatsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                Map<String, Object> stats = storageClient.getSystemStats();
                String json = "{"
                    + "\"totalFiles\":" + stats.get("totalFiles") + ","
                    + "\"totalSize\":" + stats.get("totalSize") + ","
                    + "\"activeServers\":" + stats.get("activeServers") + ","
                    + "\"leader\":\"" + stats.get("leader") + "\""
                    + "}";
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, json.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(json.getBytes());
                }
                
            } catch (Exception e) {
                logger.error("Stats error", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
    
    private class LeaderHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String leader = storageClient.getCurrentLeader();
            String response = "{\"leader\": \"" + leader + "\"}";
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    
    private class ServersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                List<String> servers = storageClient.getAliveServers();
                String json = jsonArray(servers);
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, json.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(json.getBytes());
                }
                
            } catch (Exception e) {
                logger.error("Servers error", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
    
    private class StaticFileHandler implements HttpHandler {
        private final String basePath;
        
        public StaticFileHandler(String basePath) {
            this.basePath = basePath;
        }
        
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/")) {
                path = "/index.html";
            }
            
            String filePath = basePath + path;
            InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
            
            if (is == null) {
                String response = "404 Not Found";
                exchange.sendResponseHeaders(404, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                return;
            }
            
            String contentType = getContentType(path);
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(200, 0);
            
            try (OutputStream os = exchange.getResponseBody()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                }
            }
            is.close();
        }
        
        private String getContentType(String path) {
            if (path.endsWith(".html")) return "text/html";
            if (path.endsWith(".css")) return "text/css";
            if (path.endsWith(".js")) return "application/javascript";
            if (path.endsWith(".png")) return "image/png";
            if (path.endsWith(".jpg")) return "image/jpeg";
            return "text/plain";
        }
    }
    
    // Helper methods
    private String getBoundary(HttpExchange exchange) {
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null && contentType.contains("boundary=")) {
            return contentType.split("boundary=")[1];
        }
        return null;
    }
    
    private Map<String, byte[]> parseMultipartForm(HttpExchange exchange, String boundary) throws IOException {
        if (boundary == null || boundary.isEmpty()) {
            throw new IOException("Missing multipart boundary");
        }

        InputStream is = exchange.getRequestBody();
        byte[] body = readInputStream(is);
        String raw = new String(body, StandardCharsets.ISO_8859_1);
        String delimiter = "--" + boundary;

        Map<String, byte[]> result = new HashMap<String, byte[]>();
        String[] parts = raw.split(java.util.regex.Pattern.quote(delimiter));
        for (String part : parts) {
            if (part == null || part.trim().isEmpty() || "--".equals(part.trim())) {
                continue;
            }

            int headerEnd = part.indexOf("\r\n\r\n");
            if (headerEnd < 0) {
                continue;
            }

            String headers = part.substring(0, headerEnd);
            String payload = part.substring(headerEnd + 4);

            if (payload.endsWith("\r\n")) {
                payload = payload.substring(0, payload.length() - 2);
            }
            if (payload.endsWith("--")) {
                payload = payload.substring(0, payload.length() - 2);
            }

            String name = null;
            int nameIndex = headers.indexOf("name=\"");
            if (nameIndex >= 0) {
                int start = nameIndex + 6;
                int end = headers.indexOf("\"", start);
                if (end > start) {
                    name = headers.substring(start, end);
                }
            }
            if (name == null) {
                continue;
            }

            byte[] data = payload.getBytes(StandardCharsets.ISO_8859_1);
            result.put(name, data);
        }

        return result;
    }
    
    private String extractQueryParam(String query, String param) {
        if (query == null) return null;
        for (String pair : query.split("&")) {
            String[] parts = pair.split("=");
            if (parts.length == 2 && parts[0].equals(param)) {
                try {
                    return java.net.URLDecoder.decode(parts[1], "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("UTF-8 decoding not supported", e);
                }
            }
        }
        return null;
    }
    
    private String extractJsonParam(String json, String param) {
        // Simplified JSON parsing - in production, use Jackson
        String search = "\"" + param + "\":\"";
        int start = json.indexOf(search);
        if (start == -1) return null;
        start += search.length();
        int end = json.indexOf("\"", start);
        if (end == -1) return null;
        return json.substring(start, end);
    }
    
    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
    
    private String jsonArray(List<String> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(escapeJson(list.get(i))).append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    private byte[] readInputStream(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = input.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
        return output.toByteArray();
    }
    
    public static void main(String[] args) {
        try {
            String zookeeperAddress = args.length > 0 ? args[0] : "localhost:2181";
            int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
            
            WebClientServer server = new WebClientServer(zookeeperAddress, port);
            server.start();
            
            // Keep server running
            Thread.currentThread().join();
            
        } catch (Exception e) {
            System.err.println("Failed to start web server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}