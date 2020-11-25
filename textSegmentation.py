"""
使用jieba分词
https://github.com/fxsjy/jieba
TODO: 分词改进  比如过滤标点符号等
"""
import jieba

stop_dic=[]

f=open("stop_word1.txt")
s=f.readline()
while(s!=""):
    stop_dic.append(s[:len(s)-1])
    s=f.readline()
f.close()


def filter(seg_list):
    result=[]
    for i in seg_list:
        if(i in stop_dic):
            continue
        else:
            result.append(i)
    return result

def segment(content):
    #jieba.enable_paddle()
    jieba.load_userdict("dic.txt")
    seg_list = jieba.cut(content, cut_all=False)
    result=filter(seg_list)
    res = " ".join(result)
    print(res)
    return res

#segment("我知道很多人都在捱，我也在。你们不孤独。在这个组里感受到很多安慰，真的谢谢大家了。不被认可的观点，不被祝福的感情，不被理解的人生，在这里都可以存在。谢谢。")
