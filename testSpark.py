import pyhdfs
import pymongo
import datetime
import textSegmentation
import time
import sys

HADOOP_URL = "172.20.10.3:9870"
HADOOP_USER = "root"
HADOOP_DIR = "/test1/"
client = pyhdfs.HdfsClient(hosts=HADOOP_URL, user_name=HADOOP_USER)


if __name__ == "__main__":
    file1 = HADOOP_DIR + "test.txt"
    client.create(file1, "word word word hia")
    time.sleep(10)
    file2 = HADOOP_DIR + "test2.txt"
    client.create(file2, "word word word word hia wu")
    time.sleep(10)
    file3 = HADOOP_DIR + "test3.txt"
    client.create(file3, "word word word word word da dj kas")