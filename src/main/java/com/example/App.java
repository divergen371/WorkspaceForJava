package com.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final int BUFFER_SIZE = 8 * 1024 * 1024; // 8MB buffer
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
                        jsonFiles.add(jsonFile); // JSONファイル名を記録
                        sortedNumbers.clear();
                        currentCount = 0;
                    }
                }
            }

            if (!sortedNumbers.isEmpty()) {
                String jsonFile = writeToJsonFile(sortedNumbers, currentFileIndex);
                jsonFiles.add(jsonFile); // 最後のJSONファイル名も記録
            }
        }

        // 全JSONファイルの圧縮（並行処理）
        System.out.println("全JSONファイルの圧縮を開始します...");
        int processors = Runtime.getRuntime().availableProcessors();
        int threadCount = Math.max(1, processors - 1);
        System.out.println("圧縮に使用するスレッド数: " + threadCount);
        
        ExecutorService compressExecutor = Executors.newFixedThreadPool(threadCount);
        
        try {
            // ファイルサイズを取得して、大きいファイルから処理するようにソート
            List<File> sortedFiles = jsonFiles.stream()
                .map(File::new)
                .sorted((f1, f2) -> Long.compare(f2.length(), f1.length()))
                .collect(java.util.stream.Collectors.toList());
                
            // バッチサイズを設定（一度に処理するファイル数）
            int batchSize = Math.min(threadCount, sortedFiles.size());
            
            // バッチごとに処理
            for (int i = 0; i < sortedFiles.size(); i += batchSize) {
                int endIndex = Math.min(i + batchSize, sortedFiles.size());
                List<File> batch = sortedFiles.subList(i, endIndex);
                
                System.out.println("バッチ処理開始: " + (i/batchSize + 1) + "/" + 
                                  (int)Math.ceil((double)sortedFiles.size()/batchSize));
                
                List<Future<?>> batchTasks = new ArrayList<>();
                
                // バッチ内のファイルを並行処理
                for (File jsonFile : batch) {
                    // 各タスク開始前に少し遅延を入れてI/O競合を減らす
                    try {
                        Thread.sleep(100); // 100ミリ秒の遅延
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    
                    batchTasks.add(compressExecutor.submit(() -> {
                        try {
                            System.out.println("圧縮開始: " + jsonFile.getName() + 
                                              " (サイズ: " + jsonFile.length() / 1024 / 1024 + "MB)");
                            long startTime = System.currentTimeMillis();
                            compressFile(jsonFile.getPath());
                            long endTime = System.currentTimeMillis();
                            System.out.println("圧縮完了: " + jsonFile.getName() + 
                                              " (処理時間: " + (endTime - startTime) / 1000 + "秒)");
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    }));
                }
                
                // バッチ処理後に少し待機してI/Oを安定させる
                try {
                    Thread.sleep(1000); // 1秒待機
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                // バッチ内のすべてのタスクが完了するのを待機
                for (Future<?> task : batchTasks) {
                    try {
                        task.get();
                    } catch (ExecutionException e) {
                        // 元の例外を取得して処理
                        Throwable cause = e.getCause();
                        if (cause instanceof IOException) {
                            throw (IOException) cause;
                        } else {
                            throw new IOException("圧縮処理中にエラーが発生しました: " + cause.getMessage(), cause);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("圧縮処理が中断されました", e);
                    }
                }
                
                // バッチ処理後にガベージコレクションを明示的に実行
                System.gc();
                System.out.println("バッチ処理完了: " + (i/batchSize + 1) + "/" + 
                                  (int)Math.ceil((double)sortedFiles.size()/batchSize));
            }
        } finally {
            compressExecutor.shutdown();
            try {
                if (!compressExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    compressExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                compressExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

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

        // LZMA2の圧縮設定を最適化
        LZMA2Options options = new LZMA2Options();
        options.setPreset(4); // 圧縮レベルを4に下げてI/O負荷を軽減
        options.setDictSize(8 * 1024 * 1024);
        options.setLc(3);
        options.setLp(0);
        options.setPb(2);

        // ディスクI/Oを最適化するためのバッファリング
        try (InputStream input = new java.io.BufferedInputStream(new FileInputStream(jsonFileName), BUFFER_SIZE);
                OutputStream output = new java.io.BufferedOutputStream(new FileOutputStream(xzFileName), BUFFER_SIZE);
                XZOutputStream xzOut = new XZOutputStream(output, options)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = input.read(buffer)) != -1) {
                xzOut.write(buffer, 0, n);
            }
        }

        // 元のJSONファイルを削除
        Files.delete(Path.of(jsonFileName));
        System.out.println(xzFileName + "の生成が完了しました。");
    }

    private boolean isFileComplete() {
        try {
            return Files.lines(Path.of("file.txt")).count() == 100_000_000;
        } catch (IOException e) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        App app = new App();

        // 開始時刻を記録
        long startTime = System.currentTimeMillis();

        if (!app.isFileComplete()) {
            FileGenerate.genarateFile();
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
