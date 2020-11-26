# SparkStreaming
目前大致的实现逻辑：  
在`dataToHdfs.py`中将数据从数据库存入hdfs  
在`streaming.py`中使用流式计算做词频统计，并将结果存储到hdfs中
上述两个方法同时运行(spark监听hdfs中新增的文件) 
