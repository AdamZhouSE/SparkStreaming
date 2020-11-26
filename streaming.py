"""
使用SparkStreaming做词频统计
TODO: spark是否还有其他应用
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import os


# 本地python3路径
PYSPARK_PYTHON = "/usr/bin/python3"
SPARK_MASTER = "spark://172.20.10.2:7077"
APP_NAME = "985fiveAnalysis"
DATA_DIR = "hdfs://172.20.10.3:9000/test1/"
RES_DATA_DIR = "hdfs://172.20.10.3:9000/res/"
CHECKPOINT_DIR = "hdfs://172.20.10.3:9000/checkpoint/"


def my_streaming(master, app_name, data_dir):
    sc = SparkContext(master, app_name)
    # 6秒为一个周期，时间短hadoop集群会挂掉？？？
    ssc = StreamingContext(sc, 6)
    lines = ssc.textFileStream(data_dir)
    # 词频统计
    word_counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)
    word_counts.pprint()
    # 合并为1个文件存储到hdfs
    word_counts.repartition(1).saveAsTextFiles(RES_DATA_DIR)
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    # 解决python不同版本冲突的问题
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    try:
        print("Start streaming.")
        my_streaming(SPARK_MASTER, APP_NAME, DATA_DIR)
    except KeyboardInterrupt:
        print("Stop streaming.")


