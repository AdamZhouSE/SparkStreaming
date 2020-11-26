"""
使用jieba分词https://github.com/fxsjy/jieba
停用词表存储Data/stop_word1.txt
自定义词典Data/dic.txt
"""
import jieba

STOP_WORD_PATH = "Data/stop_word1.txt"
DIC_PATH = "Data/dic.txt"

stop_dic=[]
f=open(STOP_WORD_PATH)
s=f.readline()
while s != "":
    stop_dic.append(s[:len(s)-1])
    s=f.readline()
f.close()


def filter(seg_list):
    """
    对分词结果按停用词表再进行一次筛选
    :param seg_list: 分词结果
    :return:
    """
    result=[]
    for i in seg_list:
        if i in stop_dic:
            continue
        else:
            result.append(i)
    return result


def segment(content):
    jieba.load_userdict(DIC_PATH)
    seg_list = jieba.cut(content, cut_all=False)
    result=filter(seg_list)
    res = " ".join(result)
    print(res)
    return res


if __name__ == "__main__":
    segment(" 爱国守法的爱德基金会")
