"""
将数据从mongodb存储到Hdfs
"""
import pyhdfs
import pymongo
import datetime
import textSegmentation
import time
import sys

HADOOP_URL = "172.20.10.3:9870"
HADOOP_USER = "root"
HADOOP_DIR = "/test1/"
MONGO_URL = "mongodb://localhost:27017/"
MONGO_DB = "douban"
MONGO_COL = "info"
startTime = "2020-05-10 00:00:00"
endTime = "2020-10-30 00:00:00"
client = pyhdfs.HdfsClient(hosts=HADOOP_URL, user_name=HADOOP_USER)


def save_data(mongo_url, mongo_db, mongo_col, start_time, end_time, hadoop_url, hadoop_user):
    while start_time < end_time:
        data = read_from_mongo(mongo_url, mongo_db, mongo_col, start_time)
        # 取一天的内容作为一个文件存储，若没有内容就跳过
        if len(data) > 0:
            # 分词
            data = textSegmentation.segment(data)
            # 将日期作为文件名
            filename = HADOOP_DIR + start_time[0:10] + ".txt"
            # 写入hdfs
            write_to_hdfs(filename, data.encode("utf-8"))
        else:
            print("No data in", start_time[0:10])
        # 存储文件后休眠1秒，模拟数据流
        time.sleep(1)
        start_time = add_day(start_time)
    print("All data has already been saved in Hdfs.")


# 从数据库中获取数据
def read_from_mongo(mongo_url, mongo_db, mongo_col, start_time):
    mongo_client = pymongo.MongoClient(mongo_url)
    mongo_db = mongo_client[mongo_db]
    mongo_col = mongo_db[mongo_col]
    mongo_query = {"release_time": {"$gte": start_time, "$lt": add_day(start_time)}}
    mongo_doc = mongo_col.find(mongo_query)
    res = ""
    # 这里的x是一个字典,我们仅需要帖子的标题和内容
    for x in mongo_doc:
        res += str(x["title"])
        res += str(x["content"])
    return res


# 将日期字符串增加一天
def add_day(time_str):
    time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    new_time = time + datetime.timedelta(days=1)
    return str(new_time)


def write_to_hdfs(hdfs_path, content):
    try:
        # 如果文件不存在，那么新建并写入内容，否则跳过
        if not client.exists(hdfs_path):
            client.create(hdfs_path, content)
            print("Write to =>", hdfs_path)
        else:
            print("File", hdfs_path, "already exists.")
    except pyhdfs.HdfsException:
        print(pyhdfs.HdfsException)
        sys.exit(1)
    except ConnectionError:
        print("Error!")
        sys.exit(1)


if __name__ == '__main__':
    try:
        print("Start to save data.")
        save_data(MONGO_URL, MONGO_DB, MONGO_COL, startTime, endTime, HADOOP_URL, HADOOP_USER)
    except KeyboardInterrupt:
        print("Stop to save data.")
        sys.exit(0)
