"""
使用SparkStreaming做词频统计
TODO: spark是否还有其他应用
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os


# 本地python3路径
PYSPARK_PYTHON = "/usr/bin/python3"
PYSPARK_DRIVER_PYTHON = "/usr/bin/python3"
SPARK_MASTER = "spark://172.20.10.2:7077"
APP_NAME = "985fiveAnalysis"
WORD_DATA_DIR = "hdfs://172.20.10.3:9000/word/"
RES_WORD_DATA_DIR = "hdfs://172.20.10.3:9000/wordcount_res/"
CHECKPOINT_DIR = "hdfs://172.20.10.3:9000/checkpoint/"


def my_streaming(data_dir):
    sc = SparkContext(SPARK_MASTER, APP_NAME)
    # 6秒为一个批处理舟曲
    ssc = StreamingContext(sc, 6)

    lines = ssc.textFileStream(data_dir)
    # 词频统计
    word_counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)
    word_counts.pprint()
    # 合并为1个文件存储到hdfs
    word_counts.repartition(1).saveAsTextFiles(RES_WORD_DATA_DIR)
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    # 由于Mac预装了python2.7，会和3.7冲突
    # 解决python不同版本冲突的问题
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON
    try:
        print("Start streaming.")
        my_streaming(WORD_DATA_DIR)
    except KeyboardInterrupt:
        print("Stop streaming.")


