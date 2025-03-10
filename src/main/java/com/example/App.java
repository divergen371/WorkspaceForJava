package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.io.File;
import java.util.TreeSet;
import java.util.Set;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import java.util.Iterator;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

public class App {
    private static final List<AutoCloseable> resources = new ArrayList<>();
    private static final int BUFFER_SIZE = 1024 * 1024; // 1MB buffer
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int CHUNK_SIZE = 1_000_000;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            resources.forEach(resource -> {
                try {
                    resource.close();
                } catch (Exception e) {
                    System.err.println("Failed to close resource: " + e.getMessage());
                }
            });
        }));
    }

    public void processFile() throws IOException {
        int currentFileIndex = 1;
        int currentCount = 0;
        List<String> jsonFiles = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new FileReader("file.txt"), BUFFER_SIZE)) {
            TreeSet<Long> sortedNumbers = new TreeSet<>();

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    sortedNumbers.add(Long.parseLong(line.trim()));
                    currentCount++;

                    if (currentCount >= 10_000_000) {
                        String jsonFile = writeToJsonFile(sortedNumbers, currentFileIndex++);
                        jsonFiles.add(jsonFile);  // JSONファイル名を記録
                        sortedNumbers.clear();
                        currentCount = 0;
                    }
                }
            }

            if (!sortedNumbers.isEmpty()) {
                String jsonFile = writeToJsonFile(sortedNumbers, currentFileIndex);
                jsonFiles.add(jsonFile);  // 最後のJSONファイル名も記録
            }
        }

        // 全JSONファイルの圧縮（並行処理）
        System.out.println("全JSONファイルの圧縮を開始します...");
        ExecutorService compressExecutor = Executors.newFixedThreadPool(8);
        List<Future<?>> compressionTasks = new ArrayList<>();

        try {
            for (String jsonFile : jsonFiles) {
                compressionTasks.add(compressExecutor.submit(() -> {
                    try {
                        compressFile(jsonFile);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }));
            }

            // 全ての圧縮タスクの完了を待機
            for (Future<?> task : compressionTasks) {
                task.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("圧縮処理中にエラーが発生しました: " + e.getMessage(), e);
        } finally {
            compressExecutor.shutdown();
            try {
                if (!compressExecutor.awaitTermination(1, TimeUnit.HOURS)) {
                    compressExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                compressExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("圧縮処理が完了しました。");
    }

    private String writeToJsonFile(Set<Long> numbers, int fileIndex) throws IOException {
        Path outputDir = Path.of("output");
        if (!Files.exists(outputDir)) {
            Files.createDirectory(outputDir);
        }

        String jsonFileName = String.format("output/output_part%d.json", fileIndex);

        try (JsonGenerator generator = objectMapper.getFactory().createGenerator(
                new File(jsonFileName), JsonEncoding.UTF8)) {
            generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            generator.writeArrayFieldStart("items");

            // バッチ処理で1000件ずつ処理
            List<Long> batch = new ArrayList<>(1000);
            Iterator<Long> iterator = numbers.iterator();

            while (iterator.hasNext()) {
                batch.clear();
                while (iterator.hasNext() && batch.size() < 1000) {
                    batch.add(iterator.next());
                }

                for (Long number : batch) {
                    generator.writeStartObject();
                    generator.writeNumberField("id", number);
                    generator.writeStringField("secret", UUID.randomUUID().toString());
                    generator.writeEndObject();
                }
            }

            generator.writeEndArray();
            generator.writeEndObject();
        }

        System.out.println(jsonFileName + "の生成が完了しました。");
        return jsonFileName;
    }

    private void compressFile(String jsonFileName) throws IOException {
        String xzFileName = jsonFileName + ".xz";
        
        try (InputStream input = new FileInputStream(jsonFileName);
             OutputStream output = new FileOutputStream(xzFileName);
             XZOutputStream xzOut = new XZOutputStream(output, new LZMA2Options())) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = input.read(buffer)) != -1) {
                xzOut.write(buffer, 0, n);
            }
        }

        // 元のJSONファイルを削除
        Files.delete(Path.of(jsonFileName));
        System.out.println(xzFileName + "の生成が完了しました。");
        // 不要なreturn文を削除
    }

    private boolean isFileComplete() {
        try {
            return Files.lines(Path.of("file.txt")).count() == 100_000_000;
        } catch (IOException e) {
            return false;
        }
    }

    private void generateFile() {
        if (isFileComplete()) {
            System.out.println("ファイルは既に生成済みです。");
            return;
        }
        
        // 既存のファイルを削除して新規作成
        try {
            Files.deleteIfExists(Path.of("file.txt"));
        } catch (IOException e) {
            System.err.println("既存ファイルの削除に失敗しました: " + e.getMessage());
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();

        try {
            // 順番に処理するために、チャンクを順番に生成
            for (int chunk = 0; chunk < 100; chunk++) {
                final int startNum = chunk * CHUNK_SIZE + 1;
                futures.add(executor.submit(() -> writeChunk(startNum)));
            }

            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            System.err.println("ファイル生成中にエラーが発生しました: " + e.getMessage());
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("ファイル生成が完了しました！");
    }

    private void writeChunk(int startNum) {
        String tempFile = String.format("temp_%d.txt", startNum);
        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(tempFile), BUFFER_SIZE)) {
            for (int i = 0; i < CHUNK_SIZE; i++) {
                writer.write(String.valueOf(startNum + i));
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // ここで一時ファイルをメインファイルに追記する
        // 同期化して順番に書き込みを保証する
        synchronized (App.class) {
            try {
                Files.write(
                        Path.of("file.txt"),
                        Files.readAllBytes(Path.of(tempFile)),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
                Files.delete(Path.of(tempFile));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        App app = new App();
        
        // 開始時刻を記録
        long startTime = System.currentTimeMillis();
        
        if (!app.isFileComplete()) {
            app.generateFile();
        }
        app.processFile();
        
        // 終了時刻を記録し、処理時間を計算
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        
        // 処理時間を分、秒、ミリ秒に変換
        long minutes = totalTime / (1000 * 60);
        long seconds = (totalTime % (1000 * 60)) / 1000;
        long milliseconds = totalTime % 1000;
        
        System.out.printf("総処理時間: %d分 %d秒 %dミリ秒%n", 
            minutes, seconds, milliseconds);
    }
}
