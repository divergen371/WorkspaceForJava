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
   - テキストファイルから読み取った値をIDとし、IDに紐づくユニークな値を生成を生成

``` Java
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
