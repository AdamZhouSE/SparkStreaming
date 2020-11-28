"""
使用jieba分词https://github.com/fxsjy/jieba
停用词表存储Data/stop_word1.txt
自定义词典Data/dic.txt
"""
import jieba
import re
import jieba.posseg as pseg

STOP_WORD_PATH = "..\\Data\\stop_word1.txt"
DIC_PATH = "..\\Data\\dic.txt"

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
    for word,flag in seg_list:
        print("%s %s"%(word,flag))
        if (flag=="x" or flag=="n" or flag=='vn')and len(word)!=1 and not(word in stop_dic):
            result.append(word)
    return result


def segment(content):
    jieba.load_userdict(DIC_PATH)
    seg_list = pseg.cut(content)
    result=filter(seg_list)
    print(" ".join(result))
    return result




if __name__ == "__main__":
    segment("我又被吓到，然后我再看书就看不进去了??整个人就崩溃了??我有想过换到图书馆，可是我书很多，图书馆现在也没有考研书柜可以给我用了。所以我基本上只能在考研教室学习??我不是酸她，我只是希望她在考研期间不要来找我，也不要来考研教室，不要和我一起吃饭??虽然我知道我的想法很自私，要别人迁就我，还很玻璃心，对不起！对不起！对不起！??可是我现在真的很崩溃，有人可以给我一些建议吗？")
