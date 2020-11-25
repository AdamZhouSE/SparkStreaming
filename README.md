# SparkStreaming
目前大致的实现逻辑：  
在`dataToHdfs.py`中将数据从数据库存入hdfs  
在`streaming.py`中使用流式计算做词频统计  
上述两个方法同时运行（spark监听hdfs中新增的文件），可用`testSpark.py & streaming.py`替代进行测试  
在`getWordCloud.py`中读取出spark的处理结果进行处理，最终得到一个记录词频的字典
