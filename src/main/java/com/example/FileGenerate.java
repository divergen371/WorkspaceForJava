package com.example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Locale;

public class FileGenerate {
    public static void genarateFile() {
        // 出力ファイルの設定
        File outputFile = new File("numbers_1_to_100million.txt");

        // カウンタと合計の定義
        final int TOTAL = 100_000_000;
        final int BATCH_SIZE = 1_000_000; // 一度に処理する行数
        final int REPORT_INTERVAL = 5_000_000; // 進捗報告の間隔

        // 数値フォーマット（読みやすさ用）
        NumberFormat formatter = NumberFormat.getInstance(Locale.getDefault());

        System.out.println("ファイル生成を開始します...");
        long startTime = System.currentTimeMillis();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (int i = 1; i <= TOTAL; i++) {
                // 数値を書き込む
                writer.write(Integer.toString(i));
                writer.newLine();

                // バッファをフラッシュ（一定間隔で）
                if (i % BATCH_SIZE == 0) {
                    writer.flush();
                }

                // 進捗状況の報告
                if (i % REPORT_INTERVAL == 0 || i == TOTAL) {
                    System.out.println("進捗状況: " + formatter.format(i) + " / "
                            + formatter.format(TOTAL) + " ("
                            + String.format("%.2f", (double) i / TOTAL * 100) + "%)");
                }
            }

            // 最終フラッシュ
            writer.flush();

            long endTime = System.currentTimeMillis();
            double elapsedTimeInSeconds = (endTime - startTime) / 1000.0;

            System.out.println("ファイル生成が完了しました。");
            System.out.println("ファイルパス: " + outputFile.getAbsolutePath());
            System.out.println("処理時間: " + String.format("%.2f", elapsedTimeInSeconds) + "秒");

            // ファイルサイズを計算して表示
            double fileSizeInMB = outputFile.length() / (1024.0 * 1024.0);
            System.out.println("ファイルサイズ: " + String.format("%.2f", fileSizeInMB) + " MB");

        } catch (IOException e) {
            System.err.println("エラーが発生しました: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
