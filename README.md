## Getting Started

Welcome to the VS Code Java world. Here is a guideline to help you get started to write Java code in Visual Studio Code.

## Folder Structure

The workspace contains two folders by default, where:

- `src`: the folder to maintain sources
- `lib`: the folder to maintain dependencies

Meanwhile, the compiled output files will be generated in the `bin` folder by default.

> If you want to customize the folder structure, open `.vscode/settings.json` and update the related settings there.

## Dependency Management

The `JAVA PROJECTS` view allows you to manage your dependencies. More details can be found [here](https://github.com/microsoft/vscode-java-dependency#manage-dependencies).

## プロジェクト概要

このプロジェクトは大規模データ処理のサンプルアプリケーションです。以下の機能を実装しています：

1. 1億行のテキストファイル生成
2. 数値データのソートと処理
3. JSONファイルへの出力
   - テキストファイルから読み取った値をIDとし、IDに紐づくユニークな値を生成

``` JSON
 {
  "items" : [ {
    "id" : 1,
    "secret" : "53bfbbb7-f2f6-4852-bd56-1d5aa754c8b8"
  }, {
    "id" : 2,
    "secret" : "fe899e20-322e-4bc4-aa13-5965766b1a7c"
  }, {
    "id" : 3,
    "secret" : "2384fef8-227b-457b-af24-5d45947f6d5f" 
  }  
]} 
 
```

4. 並列処理によるXZ圧縮

**(注) これは超適当な実装です。**

## 実行方法

### 前提条件

- Java 23
- Maven

### ビルドと実行

```bash
# プロジェクトのビルド
mvn clean package

# 実行
java -jar target/java-test-1.0-SNAPSHOT.jar
```

## 動作

``` Plain Text
ファイル生成を開始します...
進捗状況: 5,000,000 / 100,000,000 (5.00%)
進捗状況: 10,000,000 / 100,000,000 (10.00%)
進捗状況: 15,000,000 / 100,000,000 (15.00%)
進捗状況: 20,000,000 / 100,000,000 (20.00%)
進捗状況: 25,000,000 / 100,000,000 (25.00%)
進捗状況: 30,000,000 / 100,000,000 (30.00%)
進捗状況: 35,000,000 / 100,000,000 (35.00%)
進捗状況: 40,000,000 / 100,000,000 (40.00%)
進捗状況: 45,000,000 / 100,000,000 (45.00%)
進捗状況: 50,000,000 / 100,000,000 (50.00%)
進捗状況: 55,000,000 / 100,000,000 (55.00%)
進捗状況: 60,000,000 / 100,000,000 (60.00%)
進捗状況: 65,000,000 / 100,000,000 (65.00%)
進捗状況: 70,000,000 / 100,000,000 (70.00%)
進捗状況: 75,000,000 / 100,000,000 (75.00%)
進捗状況: 80,000,000 / 100,000,000 (80.00%)
進捗状況: 85,000,000 / 100,000,000 (85.00%)
進捗状況: 90,000,000 / 100,000,000 (90.00%)
進捗状況: 95,000,000 / 100,000,000 (95.00%)
進捗状況: 100,000,000 / 100,000,000 (100.00%)
ファイル生成が完了しました。
ファイルパス: ~/Java_Trae_Test/WorkspaceForJava/file.txt
処理時間: 2.44秒
ファイルサイズ: 847.71 MB
output/output_part1.jsonの生成が完了しました。
output/output_part2.jsonの生成が完了しました。
output/output_part3.jsonの生成が完了しました。
output/output_part4.jsonの生成が完了しました。
output/output_part5.jsonの生成が完了しました。
output/output_part6.jsonの生成が完了しました。
output/output_part7.jsonの生成が完了しました。
output/output_part8.jsonの生成が完了しました。
output/output_part9.jsonの生成が完了しました。
output/output_part10.jsonの生成が完了しました。
全JSONファイルの圧縮を開始します...
圧縮に使用するスレッド数: 7
バッチ処理開始: 1/2
圧縮開始: output_part10.json (サイズ: 782MB)
圧縮開始: output_part2.json (サイズ: 782MB)
圧縮開始: output_part3.json (サイズ: 782MB)
圧縮開始: output_part4.json (サイズ: 782MB)
圧縮開始: output_part5.json (サイズ: 782MB)
圧縮開始: output_part6.json (サイズ: 782MB)
圧縮開始: output_part7.json (サイズ: 782MB)
output/output_part10.json.xzの生成が完了しました。
圧縮完了: output_part10.json (処理時間: 384秒)
output/output_part4.json.xzの生成が完了しました。
圧縮完了: output_part4.json (処理時間: 385秒)
output/output_part7.json.xzの生成が完了しました。
圧縮完了: output_part7.json (処理時間: 385秒)
output/output_part6.json.xzの生成が完了しました。
圧縮完了: output_part6.json (処理時間: 385秒)
output/output_part3.json.xzの生成が完了しました。
圧縮完了: output_part3.json (処理時間: 386秒)
output/output_part2.json.xzの生成が完了しました。
圧縮完了: output_part2.json (処理時間: 386秒)
output/output_part5.json.xzの生成が完了しました。
圧縮完了: output_part5.json (処理時間: 386秒)
バッチ処理完了: 1/2
バッチ処理開始: 2/2
圧縮開始: output_part8.json (サイズ: 782MB)
圧縮開始: output_part9.json (サイズ: 782MB)
圧縮開始: output_part1.json (サイズ: 771MB)
output/output_part9.json.xzの生成が完了しました。
圧縮完了: output_part9.json (処理時間: 257秒)
output/output_part8.json.xzの生成が完了しました。
圧縮完了: output_part8.json (処理時間: 258秒)
output/output_part1.json.xzの生成が完了しました。
圧縮完了: output_part1.json (処理時間: 258秒)
バッチ処理完了: 2/2
総処理時間: 11分 29秒 325ミリ秒
```
