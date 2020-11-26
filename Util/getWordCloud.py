"""
从hdfs中读取spark处理后的结果，生成词云
"""
import pyhdfs

HADOOP_URL = "172.20.10.3:9870"
HADOOP_USER = "root"
HADOOP_DIR = "/res/"
FILENAME = "part-00000"
client = pyhdfs.HdfsClient(hosts=HADOOP_URL, user_name=HADOOP_USER)
data = dict()


def get_data():
    file_list = client.listdir(HADOOP_DIR)
    for f in file_list:
        pathname = HADOOP_DIR + f + "/" + FILENAME
        # 存在该文件
        if client.exists(pathname):
            file_status = client.list_status(pathname)
            # 文件不为空
            if file_status[0]["length"] > 0:
                response = client.open(pathname)
                content = ""
                for c in response:
                    content += str(c, "utf-8")
                # 对数据进行处理
                parse_data(content)


def parse_data(content):
    """
    传入的处理结果格式：一行一个字符串，形如('word', 1)\n，将其按照key-value的形式加入字典
    :param content: spark处理结果的字符串形式
    :return:
    """
    tuple_list = content.split("\n")
    for str_tuple in tuple_list:
        if len(str_tuple) > 0:
            # 将字符串转换为元组
            t = eval(str_tuple)
            key = t[0]
            value = t[1]
            if key in data:
                data[key] += value
            else:
                data[key] = value


if __name__ == "__main__":
    get_data()
    print(data)