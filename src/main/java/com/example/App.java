package com.example;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class App {
    private static final List<AutoCloseable> resources            = new ArrayList<>();
    private static final int                 BUFFER_SIZE          = 8 * 1024 * 1024; // 8MB buffer
    private static final ObjectMapper        objectMapper         = new ObjectMapper();
    // 圧縮用のバッファ設定
    private static final int                 COMPRESS_BUFFER_SIZE = 64 * 1024;

    static {
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> resources.forEach(resource -> {
                   try {
                       resource.close();
                   } catch (Exception e) {
                       System.err.println("Failed to close resource: " + e.getMessage());
                   }
               })));
    }

    /**
     * mainメソッド
     *
     * @param args コマンドライン引数
     * @throws Exception 例外
     */
    public static void main(String[] args) throws Exception {
        App app = new App();

        // 開始時刻を記録
        long startTime = System.currentTimeMillis();

        if (! app.isFileComplete()) {
            FileGenerate.generateFile();
        }
        app.processFile();

        // 終了時刻を記録し、処理時間を計算
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // 処理時間を分、秒、ミリ秒に変換
        long minutes = totalTime / (1000 * 60);
        long seconds = (totalTime % (1000 * 60)) / 1000;
        long milliseconds = totalTime % 1000;

        System.out.printf(
                "総処理時間: %d分 %d秒 %dミリ秒%n",
                minutes, seconds, milliseconds);
    }

    /**
     * ファイルをJSONファイルに分割し、各ファイルを圧縮する
     *
     * <p>このメソッドは、{@link FileGenerate#generateFile()}によって生成されたファイルを
     * JSONファイルに分割し、各ファイルを圧縮します。
     *
     * <p>このメソッドは、{@link #main(String[])}メソッドで呼び出されます。
     *
     * @throws IOException 入出力例外
     */
    public void processFile() throws IOException {
        int currentFileIndex = 1;
        List<String> jsonFiles = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new FileReader("file.txt"), BUFFER_SIZE)
        ) {
            ArrayList<Long> numberBuffer = new ArrayList<>(10_000_000);

            String line;
            while ((line = reader.readLine()) != null) {
                if (! line.trim().isEmpty()) {
                    numberBuffer.add(Long.parseLong(line.trim()));

                    if (numberBuffer.size() >= 10_000_000) {
                        Collections.sort(numberBuffer);
                        String jsonFile = writeToJsonFile(
                                numberBuffer,
                                currentFileIndex++);
                        jsonFiles.add(jsonFile); // JSONファイル名を記録
                        numberBuffer.clear();

                    }
                }
            }

            if (! numberBuffer.isEmpty()) {
                String jsonFile = writeToJsonFile(
                        numberBuffer,
                        currentFileIndex);
                jsonFiles.add(jsonFile); // 最後のJSONファイル名も記録
            }
        }

        // 全JSONファイルの圧縮（並行処理）
        System.out.println("全JSONファイルの圧縮を開始します...");
        int processors = Runtime.getRuntime().availableProcessors();
        int threadCount = Math.max(1, processors - 3);
        System.out.println("圧縮に使用するスレッド数: " + threadCount);

        try (ExecutorService compressExecutor = Executors.newFixedThreadPool(
                threadCount)
        ) {

            try {
                // ファイルサイズを取得して、大きいファイルから処理するようにソート
                List<File> sortedFiles = jsonFiles.stream()
                                                  .map(File::new)
                                                  .sorted((f1, f2) -> Long.compare(
                                                          f2.length(),
                                                          f1.length()))
                                                  .collect(java.util.stream.Collectors.toList());

                // バッチサイズを設定（一度に処理するファイル数）
                int batchSize = Math.min(threadCount, sortedFiles.size());

                // バッチごとに処理
                for (int i = 0; i < sortedFiles.size(); i += batchSize) {
                    int endIndex = Math.min(i + batchSize, sortedFiles.size());
                    List<File> batch = sortedFiles.subList(i, endIndex);

                    System.out.println("バッチ処理開始: " + (i / batchSize + 1) + "/" +
                                       (int) Math.ceil((double) sortedFiles.size() / batchSize));

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
                                throw new IOException(
                                        "圧縮処理中にエラーが発生しました: " + cause.getMessage(),
                                        cause);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(
                                    "圧縮処理が中断されました",
                                    e);
                        }
                    }

                    // バッチ処理後にガベージコレクションを明示的に実行
                    System.gc();
                    System.out.println("バッチ処理完了: " + (i / batchSize + 1) + "/" +
                                       (int) Math.ceil((double) sortedFiles.size() / batchSize));
                }
            } finally {
                compressExecutor.shutdown();
                try {
                    if (! compressExecutor.awaitTermination(
                            60,
                            TimeUnit.SECONDS)) {
                        compressExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    compressExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    /**
     * 並列処理でJSONファイルを生成
     *
     * <p>このメソッドは、{@link #processFile()}メソッドで呼び出されます。
     *
     * @param numberBuffer 並列処理の対象となる数値データのバッファー
     * @param fileIndex    生成するJSONファイル名に使用するインデックス
     * @return 生成されたJSONファイル名
     * @throws IOException 入出力例外
     */
    private String writeToJsonFile(ArrayList<Long> numberBuffer, int fileIndex)
            throws IOException {
        Path outputDir = Path.of("output");
        if (! Files.exists(outputDir)) {
            Files.createDirectory(outputDir);
        }

        String jsonFileName = String.format(
                "output/output_part%d.json",
                fileIndex);

        try (RandomAccessFile raf = new RandomAccessFile(
                jsonFileName,
                "rw"); FileChannel channel = raf.getChannel()
        ) {
            long estimatedSize = numberBuffer.size() * 120L + 10_000L;
            MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    estimatedSize);

            try (OutputStream outputStream = new MappedByteBufferOutputStream(
                    buffer);
                    JsonGenerator generator = objectMapper.getFactory()
                                                          .createGenerator(
                                                                  outputStream,
                                                                  JsonEncoding.UTF8)
            ) {
                generator.useDefaultPrettyPrinter();
                generator.writeStartObject();
                generator.writeArrayFieldStart("items");

                // バッチ処理で1000件ずつ処理
                List<Long> batch = new ArrayList<>(1000);
                Iterator<Long> iterator = numberBuffer.iterator();

                while (iterator.hasNext()) {
                    batch.clear();
                    while (iterator.hasNext() && batch.size() < 1000) {
                        batch.add(iterator.next());
                    }

                    for (Long number : batch) {
                        generator.writeStartObject();
                        generator.writeNumberField("id", number);
                        generator.writeStringField(
                                "secret",
                                UUID.randomUUID()
                                    .toString());
                        generator.writeEndObject();
                    }
                }

                generator.writeEndArray();
                generator.writeEndObject();
            }
            channel.truncate(buffer.position());
        } catch (IOException e) {
            Files.deleteIfExists(Path.of(jsonFileName));
            throw e;
        }

        System.out.println(jsonFileName + "の生成が完了しました。");
        return jsonFileName;
    }

    /**
     * 並列処理でJSONファイルをXZ圧縮
     *
     * <p>このメソッドは、{@link #processFile()}メソッドで呼び出されます。
     *
     * @param jsonFileName XZ圧縮するJSONファイル名
     * @throws IOException 入出力例外
     */
    private void compressFile(String jsonFileName) throws IOException {
        String xzFileName = jsonFileName + ".xz";

        // LZMA2の圧縮設定を最適化
        LZMA2Options options = new LZMA2Options();
        options.setPreset(4); // 圧縮レベルを4に下げてI/O負荷を軽減
        options.setDictSize(32 * 1024 * 1024);
        options.setLc(3);
        options.setLp(0);
        options.setPb(2);
        options.setMode(LZMA2Options.MODE_FAST);

        // ディスクI/Oを最適化するためのバッファリング
        try (InputStream input = new java.io.BufferedInputStream(
                new FileInputStream(jsonFileName), BUFFER_SIZE);
                OutputStream output = new java.io.BufferedOutputStream(
                        new FileOutputStream(xzFileName), BUFFER_SIZE);
                XZOutputStream xzOut = new XZOutputStream(output, options)
        ) {

            byte[] buffer = new byte[COMPRESS_BUFFER_SIZE];
            int n;
            while ((n = input.read(buffer)) != - 1) {
                xzOut.write(buffer, 0, n);
            }
        }

        // 元のJSONファイルを削除
        Files.delete(Path.of(jsonFileName));
        System.out.println(xzFileName + "の生成が完了しました。");
    }

    /**
     * {@code file.txt}が完全に生成されているかどうかを確認します。
     *
     * <p>このメソッドは、{@link #main(String[])}メソッドで呼び出されます。
     *
     * @return {@code file.txt}が完全に生成されている場合は{@code true}を返し、そうでない場合は{@code false}を返します。
     */
    private boolean isFileComplete() {
        // file.txtが存在すれば、行数をカウント
        // 存在しなければfalseを返す
        try (Stream<String> lines = Files.lines(Path.of("file.txt"))) {
            return lines.count() == 10_000_000;
        } catch (IOException e) {
            return false;
        }
    }


    private static class MappedByteBufferOutputStream extends OutputStream {
        private final MappedByteBuffer buffer;

        public MappedByteBufferOutputStream(MappedByteBuffer buffer) {
            this.buffer = buffer;
        }


        /**
         * {@inheritDoc}
         *
         * <p>この実装は、指定された1バイトをバッファに書き込みます。
         *
         * @param b 1バイトの値
         */
        @Override
        public void write(int b) {
            buffer.put((byte) b);
        }

        /**
         * {@inheritDoc}
         *
         * <p>この実装はバッファに指定された範囲のバイトを書き込みます。
         */
        @Override
        public void write(byte[] b, int off, int len) {
            buffer.put(b, off, len);
        }
    }
}
