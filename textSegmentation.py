"""
使用jieba分词
https://github.com/fxsjy/jieba
TODO: 分词改进  比如过滤标点符号等
"""
import jieba


def segment(content):
    jieba.enable_paddle()
    seg_list = jieba.cut(content, use_paddle=True)
    res = " ".join(seg_list)
    # print(" ".join(seg_list))
    return res