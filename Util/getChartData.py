"""
在词频统计结束之后，获取词频统计的结果，将其存储到一个csv文件中
用于制作动态柱状图
"""
import pyhdfs
import datetime
import csv
HADOOP_URL = "172.20.10.3:9870"
HADOOP_USER = "root"
HADOOP_DIR = "/wordcount_res/"
FILENAME = "part-00000"
client = pyhdfs.HdfsClient(hosts=HADOOP_URL, user_name=HADOOP_USER)
data_dict = {}


def get_data():
    # res里存储一个数组[日期, 词频字典]
    res = []
    # 获取目录下所有文件并遍历
    file_list = client.listdir(HADOOP_DIR)
    start_time = "2020-05-10 00:00:00"
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
                # 对数据进行处理,返回词频字典
                parse_data(content)
                save_data_dict = {}
                for k in data_dict:
                    save_data_dict[k] = data_dict[k]
                res.append([start_time[0:10], save_data_dict])
                print([start_time[0:10], save_data_dict])
                start_time = add_day(start_time)
    return res


def parse_data(content):
    """
    传入的处理结果格式：一行一个字符串，形如('word', 1)\n，将其按照key-value的形式加入字典
    :param content: spark处理结果的字符串形式
    :return: data 字典存储了词频统计结果
    """
    tuple_list = content.split("\n")
    for str_tuple in tuple_list:
        if len(str_tuple) > 0:
            # 将字符串转换为元组
            t = eval(str_tuple)
            key = t[0]
            value = t[1]
            if key in data_dict:
                data_dict[key] += value
            else:
                data_dict[key] = value


# 将日期字符串增加一天
def add_day(time_str):
    time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    new_time = time + datetime.timedelta(days=1)
    return str(new_time)


def save_as_file(info):
    with open("day_wordcount.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "type", "value", "date"])
        for day_info in info:
            cur_day = day_info[0]
            data = day_info[1]
            for k in data:
                # 筛掉小数据，避免绘制程序过载
                if data[k] < 50:
                    continue
                writer.writerow([k, k, data[k], cur_day])


if __name__ == "__main__":
    day_wordcount = get_data()
    save_as_file(day_wordcount)