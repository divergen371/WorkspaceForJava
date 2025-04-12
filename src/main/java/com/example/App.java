package com.example;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class App {
    private static final List<AutoCloseable> resources            = new ArrayList<>();
    private static final int                 BUFFER_SIZE          = 8 * 1024 * 1024; // 8MB buffer
    private static final long                MAX_MAPPING_SIZE     = 1024 * 1024 * 1024L; // 1GB maximum mapping size
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
     * <p>JVMオプション推奨設定:
     * -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xmx4g
     *
     * @throws IOException 入出力例外
     */
    public void processFile() throws IOException {
        int currentFileIndex = 1;
        List<String> jsonFiles = new ArrayList<>();

        // 方法1: より小さなバッファサイズを使用
        try (BufferedReader reader = new BufferedReader(
                new FileReader("file.txt"), BUFFER_SIZE)
        ) {
            // バッファサイズを10分の1に削減
            ArrayList<Long> numberBuffer = new ArrayList<>(1_000_000);

            String line;
            while ((line = reader.readLine()) != null) {
                if (! line.trim().isEmpty()) {
                    numberBuffer.add(Long.parseLong(line.trim()));

                    if (numberBuffer.size() >= 1_000_000) { // バッファサイズに合わせて条件も変更
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

        // 方法2: ストリーム処理を使用する場合は以下のコードを使用
        // 注意: 上記のBufferedReaderを使用する方法と同時に使用しないこと
        /*
        Path filePath = Path.of("file.txt");
        // グループごとの最大サイズ
        final int GROUP_SIZE = 1_000_000;

        // ファイルをストリーム処理し、GROUP_SIZEごとにグループ化
        Map<Integer, List<Long>> groups = Files.lines(filePath)
            .filter(line -> !line.trim().isEmpty())
            .map(Long::parseLong)
            .collect(Collectors.groupingBy(n -> (int)(n / GROUP_SIZE)));

        // 各グループを処理
        for (Map.Entry<Integer, List<Long>> entry : groups.entrySet()) {
            ArrayList<Long> numberBuffer = new ArrayList<>(entry.getValue());
            Collections.sort(numberBuffer);
            String jsonFile = writeToJsonFile(numberBuffer, currentFileIndex++);
            jsonFiles.add(jsonFile);
        }
        */

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
            // バッファオーバーフローを防ぐために、十分なサイズ見積もりに戻す
            // UUIDなどのデータも考慮して大きめに見積もる
            long estimatedSize = numberBuffer.size() * 150L + 50_000L;
            System.out.println("ファイルサイズ見積もり: " + (estimatedSize / (1024 * 1024)) + "MB");

            // 大きなファイルの場合はメモリマッピングを分割
            final long MAX_MAPPING_SIZE = 1024 * 1024 * 1024L; // 1GB
            MappedByteBuffer buffer;

            if (estimatedSize > MAX_MAPPING_SIZE) {
                // 大きなファイルの場合は最大マッピングサイズに制限
                System.out.println("大きなファイルのため、メモリマッピングを" + MAX_MAPPING_SIZE / (1024*1024) + "MBに制限します");

                // ファイルサイズが大きい場合は、通常のファイル出力に切り替える
                System.out.println("メモリマッピングの代わりに通常のファイル出力を使用します");
                return writeToJsonFileWithoutMapping(numberBuffer, fileIndex);
            } else {
                buffer = channel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        estimatedSize);
            }

            // オフヒープメモリを活用したバッファの使用
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

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

                // バッチ処理で1000件ずつ処理 - メモリ効率を向上
                final int WRITE_BATCH_SIZE = 1000;
                List<Long> batch = new ArrayList<>(WRITE_BATCH_SIZE);
                Iterator<Long> iterator = numberBuffer.iterator();

                // 処理済みの数を追跡
                long processedCount = 0;
                long totalCount = numberBuffer.size();

                while (iterator.hasNext()) {
                    batch.clear();
                    while (iterator.hasNext() && batch.size() < WRITE_BATCH_SIZE) {
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

                    // 進捗状況を更新
                    processedCount += batch.size();
                    if (processedCount % 100000 == 0 || processedCount == totalCount) {
                        System.out.printf("JSONデータ生成進捗: %.1f%% (%d/%d)%n",
                            (double)processedCount/totalCount*100, processedCount, totalCount);
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
        File jsonFile = new File(jsonFileName);
        long fileSize = jsonFile.length();

        // LZMA2の圧縮設定を最適化
        LZMA2Options options = new LZMA2Options();
        options.setPreset(4); // 圧縮レベルを4に下げてI/O負荷を軽減
        options.setDictSize(32 * 1024 * 1024);
        options.setLc(3);
        options.setLp(0);
        options.setPb(2);
        options.setMode(LZMA2Options.MODE_FAST);

        // ファイルサイズに基づいてバッファサイズを調整
        int optimalBufferSize = calculateOptimalBufferSize(fileSize);
        System.out.println("圧縮用バッファサイズ: " + (optimalBufferSize / 1024) + "KB");

        // ディスクI/Oを最適化するためのバッファリング
        try (InputStream input = new java.io.BufferedInputStream(
                new FileInputStream(jsonFileName), optimalBufferSize);
                OutputStream output = new java.io.BufferedOutputStream(
                        new FileOutputStream(xzFileName), optimalBufferSize);
                XZOutputStream xzOut = new XZOutputStream(output, options)
        ) {
            // DirectByteBufferを使用してオフヒープメモリを活用
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(COMPRESS_BUFFER_SIZE);
            byte[] buffer = new byte[COMPRESS_BUFFER_SIZE];
            int n;
            long totalBytesRead = 0;

            while ((n = input.read(buffer)) != -1) {
                // DirectByteBufferにデータをコピー
                directBuffer.clear();
                directBuffer.put(buffer, 0, n);
                directBuffer.flip();

                // バッファからデータを読み取って圧縮
                xzOut.write(buffer, 0, n);

                // 進捗状況を更新
                totalBytesRead += n;
                if (totalBytesRead % (10 * 1024 * 1024) == 0 || totalBytesRead == fileSize) { // 10MBごとに報告
                    System.out.printf("圧縮進捗: %.1f%% (%d/%d バイト)%n",
                        (double)totalBytesRead/fileSize*100, totalBytesRead, fileSize);
                }
            }
        }

        // 元のJSONファイルを削除
        Files.delete(Path.of(jsonFileName));
        System.out.println(xzFileName + "の生成が完了しました。");
    }

    /**
     * ファイルサイズに基づいて最適なバッファサイズを計算
     *
     * @param fileSize ファイルサイズ（バイト）
     * @return 最適なバッファサイズ（バイト）
     */
    private int calculateOptimalBufferSize(long fileSize) {
        // 小さなファイル: 512KB、中サイズファイル: 2MB、大きなファイル: 8MB
        if (fileSize < 10 * 1024 * 1024) { // 10MB未満
            return 512 * 1024;
        } else if (fileSize < 100 * 1024 * 1024) { // 100MB未満
            return 2 * 1024 * 1024;
        } else {
            return BUFFER_SIZE; // 8MB
        }
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


    /**
     * メモリマッピングを使用せずにJSONファイルを生成する代替メソッド
     * 大きなファイルの場合に使用される
     *
     * @param numberBuffer 数値データのバッファー
     * @param fileIndex 生成するJSONファイル名に使用するインデックス
     * @return 生成されたJSONファイル名
     * @throws IOException 入出力例外
     */
    private String writeToJsonFileWithoutMapping(ArrayList<Long> numberBuffer, int fileIndex)
            throws IOException {
        Path outputDir = Path.of("output");
        if (! Files.exists(outputDir)) {
            Files.createDirectory(outputDir);
        }

        String jsonFileName = String.format(
                "output/output_part%d.json",
                fileIndex);

        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(jsonFileName), BUFFER_SIZE);
             JsonGenerator generator = objectMapper.getFactory()
                     .createGenerator(bos, JsonEncoding.UTF8)
        ) {
            generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            generator.writeArrayFieldStart("items");

            // バッチ処理で1000件ずつ処理 - メモリ効率を向上
            final int WRITE_BATCH_SIZE = 1000;
            List<Long> batch = new ArrayList<>(WRITE_BATCH_SIZE);
            Iterator<Long> iterator = numberBuffer.iterator();

            // 処理済みの数を追跡
            long processedCount = 0;
            long totalCount = numberBuffer.size();

            while (iterator.hasNext()) {
                batch.clear();
                while (iterator.hasNext() && batch.size() < WRITE_BATCH_SIZE) {
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

                // 進捗状況を更新
                processedCount += batch.size();
                if (processedCount % 100000 == 0 || processedCount == totalCount) {
                    System.out.printf("JSONデータ生成進捗: %.1f%% (%d/%d)%n",
                        (double)processedCount/totalCount*100, processedCount, totalCount);
                }
            }

            generator.writeEndArray();
            generator.writeEndObject();
        } catch (IOException e) {
            Files.deleteIfExists(Path.of(jsonFileName));
            throw e;
        }

        System.out.println(jsonFileName + "の生成が完了しました。");
        return jsonFileName;
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
            try {
                buffer.put((byte) b);
            } catch (BufferOverflowException e) {
                throw new RuntimeException("バッファオーバーフロー: バッファサイズが不足しています", e);
            }
        }

        /**
         * {@inheritDoc}
         *
         * <p>この実装はバッファに指定された範囲のバイトを書き込みます。
         */
        @Override
        public void write(byte[] b, int off, int len) {
            try {
                buffer.put(b, off, len);
            } catch (BufferOverflowException e) {
                throw new RuntimeException("バッファオーバーフロー: バッファサイズが不足しています", e);
            }
        }
    }
}
