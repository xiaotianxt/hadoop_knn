# Hadoop-KNN 检索
## 项目概述
### Class
- Main: 主程序
- Indexer: 生成geohash
- Searcher: 进行KNN查找
## 构建 Maven 项目并打包 jar
```bash
mvn clean package
```
## Hadoop 部署任务
```bash
hadoop jar target/hadoop_knn_.jar com.knn.Main
```

---
上传文件可以直接使用 `upload_file.sh`
