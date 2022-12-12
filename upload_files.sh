hadoop fs -mkdir /knn
hadoop fs -put ./data/points.txt /knn
hadoop fs -put ./data/query.txt /knn
hadoop fs -ls /knn