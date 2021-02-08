'''
description: auto_news_cleaning
date: 2019-10-06
author: jtx
'''






#-*- encoding:utf-8 -*-
from pyhanlp import *
import pymongo
from ltp_service import LtpService
import re
import time




client = pymongo.MongoClient('...', 0)
db1 = client.lz_data_gather_ai
col1 = db1.company_basic_auto_news_2020_year

# 日期匹配
def data_match(test_str):
    # 格式匹配. 如2016-12-24与2016/12/24的日期格式.
    publish_date = ''
    date_reg_exp_list = []
    date_reg_exp = re.compile('\d{4}[-/._年 ]\d{2}[-/._月 ]\d{2}')
    date_reg_exp2 = re.compile('\d{4}[-/._年 ]\d{2}')
    date_reg_exp3 = re.compile('\d{4}[-/._年 ]\d{1}[-/._月 ]\d{2}')
    date_reg_exp4 = re.compile('\d{4}[-/._年 ]\d{2}[-/._月 ]\d{1}')
    date_reg_exp_list.append(date_reg_exp)
    date_reg_exp_list.append(date_reg_exp2)
    date_reg_exp_list.append(date_reg_exp3)
    date_reg_exp_list.append(date_reg_exp4)
    # test_str= """
    #      平安夜圣诞节2016-12-24的日子与去年2015/12/24的是有不同哦.
    #      """
    matches_list = []
    matches_list_clean = []
    # 根据正则查找所有日期并返回
    for num, date_reg_exp_list_each in enumerate(date_reg_exp_list):
        # print('num:', num)
        matches_list = date_reg_exp_list_each.findall(test_str)
        if matches_list:
            # 列出并打印匹配的日期
            for match in matches_list:
                if (str(match).startswith('20')):
                    # if ('2020' in str(match)):
                    # print(match)
                    publish_date = ''
                    publish_date_linshi = ''
                    publish_date_linshi_list = []
                    if num == 0:
                        publish_date = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace('_',
                                                                                                           '-').replace(
                            '/',
                            '-').replace(
                            '.', '-').strip()
                        # print('match:',publish_date)
                        matches_list_clean.append(publish_date)
                        # print(matches_list_clean)
                    elif num == 1:
                        publish_date = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace('_',
                                                                                                           '-').replace(
                            '/',
                            '-').replace(
                            '.', '-').strip() + '-01'
                        # print('match:', publish_date)
                        matches_list_clean.append(publish_date)
                        # print(matches_list_clean)
                    elif num == 2:
                        publish_date_linshi = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace('_',
                                                                                                                  '-').replace(
                            '/',
                            '-').replace(
                            '.', '-').strip()
                        publish_date_linshi_list = publish_date_linshi.split('-')
                        for iii_num, iii in enumerate(publish_date_linshi_list):
                            if iii_num == 1:
                                publish_date += '-0' + iii
                            elif iii_num == 2:
                                publish_date += '-' + iii
                            elif iii_num == 0:
                                publish_date += iii
                        # print('match:', publish_date)
                        matches_list_clean.append(publish_date)
                        # print(matches_list_clean)
                    elif num == 3:
                        publish_date_linshi = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace(
                            '_',
                            '-').replace(
                            '/',
                            '-').replace(
                            '.', '-').strip()
                        publish_date_linshi_list = publish_date_linshi.split('-')
                        for iii_num, iii in enumerate(publish_date_linshi_list):
                            if iii_num == 2:
                                publish_date += '-0' + iii
                            elif iii_num == 1:
                                publish_date += '-' + iii
                            elif iii_num == 0:
                                publish_date += iii
                        # print('match:', publish_date)
                        matches_list_clean.append(publish_date)
                        # print(matches_list_clean)
    if matches_list:
        return matches_list, matches_list_clean
        # 2016-12-24
        # 2015/12/24
    else:
        return None, None

# 定义删除除汉字以外的函数
def remove_punctuation(line):
    line = str(line)
    if line.strip() == '':
        return ''
    r1 = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
    r2 = u"[^\u4E00-\u9FA5]"
    rule = re.compile(r2)
    line = rule.sub('', line)
    return line

# 定义删除除字母,数字，汉字以外的所有符号的函数
def remove_punctuation2(line):
    line = str(line)
    if line.strip() == '':
        return ''
    r1 = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
    r2 = u"[^“”""！？!?a-zA-Z0-9\u4E00-\u9FA5]"
    rule = re.compile(r2)
    line = rule.sub(' ', line)
    return line

# 三元组抽取
def ltp_e(i, title_list):
    triple_linshi = ''
    if title_list:
        for each in title_list:
            sentence = each
            # sentence = '电脑采用显示灵活、色彩艳丽强力巨彩LED显示屏'
            if len(sentence) > 30:
                sentence = sentence[:30]
            a = LtpService()
            triple = a.ltp_extract_triple(sentence)
            # print(triple)
            if triple:
                triple_linshi = ''
                for each2 in triple[0]:
                    triple_linshi += each2
                triple_linshi = triple_linshi.strip().replace(' ', '').replace('\n', '').replace('\r', '')
                # print(triple_linshi)
                myquery = {"url": i['url']}
                newvalues = {"$set": {"title_triple": triple_linshi}}
                col1.update_one(myquery, newvalues)
                break
            else:
                myquery = {"url": i['url']}
                newvalues = {"$set": {"title_triple": ''}}
                col1.update_one(myquery, newvalues)
    else:
        myquery = {"url": i['url']}
        newvalues = {"$set": {"title_triple": ''}}
        col1.update_one(myquery, newvalues)

    return triple_linshi

# 关键句抽取部分
def key_sentence_e(i):
    title_list = []
    url = i['url']
    text2 = i['content']
    if text2:
        print("=" * 30 + "自动摘要" + "=" * 30)
        # print(i['url'])
        # print(HanLP.extractSummary(text2, 6))
        print("-" * 70)

        a = HanLP.extractSummary(text2, 3)
        # print(type(a))

        for each in a:
            each_clean = each.replace('了', '').replace('的', '')
            # print(each_clean)
            # print(HanLP.segment(each.replace('了', '').replace('的', '')))
            title_list.append(each_clean)

        myquery = {"url": url}
        newvalues = {"$set": {"title_list": title_list}}
        col1.update_one(myquery, newvalues)
    return text2, title_list

# title清洗部分
def title_cleaning(i, title_triple):
    title1 = i['title']
    title2 = i['title_auto']
    # title_triple = ''
    # try:
    #     title_triple = i['title_triple']
    # except:
    #     pass
    # print(title_triple)

    list1 = ['上一篇：', '下一篇：', '上一篇', '下一篇', '没有了', '上一条', '下一条', '上一页', '下一页', '打印此页', '首页', '查看更多', '[ 查看更多 ]',
             'Home', 'Learn More'
        , '更多', '返回', '＋查看详情', '查看详情', '查看', '查看更多+', '了解更多']

    title1_clean_first = title1.strip().replace(' ', '').replace('\n', '').replace('\r', '')
    title1_clean = title1_clean_first
    data, data2 = data_match(title1)
    if data:
        for i2 in data:
            title1_clean = title1_clean_first.replace(i2, '')
    for i4 in list1:
        if i4 in title1_clean_first:
            title1_clean = str(title1_clean_first).replace(i4, '')
            print(title1_clean)
    for i3 in range(0, 10):
        if str(title1_clean_first) == str(i3):
            title1_clean = ''
    if not title_triple:
        myquery = {"url": i['url']}
        newvalues = {"$set": {"title_triple": ""}}
        col1.update_one(myquery, newvalues)
        newvalues = {"$set": {"title": title1_clean}}
        col1.update_one(myquery, newvalues)
    else:
        myquery = {"url": i['url']}
        newvalues = {"$set": {"title": title1_clean}}
        col1.update_one(myquery, newvalues)
    return title1, title1_clean

# title_auto清洗部分
def title_auto_cleaning(i_host, c, title1_clean):
    title1 = title1_clean
    title2 = i_host['title_auto']
    crawl_time = i_host['crawl_time']
    if not title1:
        if title2:
            pattern = r',|\.|/|;|\'|`|\[|\]|<|>|\?|:|"|\{|\}|\~|!|@|#|\$|%|\^|&|\(|\)|-|=|\_|\+|，|。|、|；|‘|’|【|】|·|！| |…|（|）|\|'
            result_list = re.split(pattern, title2)
            # print(result_list)
            for i in result_list:
                # pk = col1.find_one({'title_auto':})

                flag = 0
                d = 0
                if i:
                    # myDatetime = parser.parse(crawl_time)
                    for i2 in col1.find({'crawler.date': {'$gte': crawl_time}}):
                        d += 1
                        if c < d < (c + 5):
                            title_auto = i2['title_auto']
                            if i in title_auto:
                                title_auto_clean = title_auto.replace(i, '')
                                if title_auto_clean.endswith('_') or title_auto_clean.endswith(
                                        '-') or title_auto_clean.endswith('|'):
                                    title_auto_clean = title_auto_clean[:-1]
                                myquery = {"url": i2["url"]}
                                newvalues = {"$set": {"title_auto": title_auto_clean}}
                                # print('更新成功')
                                # print(title_auto_clean)
                                col1.update_one(myquery, newvalues)
                                flag = 1
                                if title_auto_clean.endswith('_') or title_auto_clean.endswith(
                                        '-') or title_auto_clean.endswith(
                                        '|'):
                                    title_auto_clean = title_auto_clean[:-1]
                                    myquery = {"url": i2["url"]}
                                    newvalues = {"$set": {"title_auto": title_auto_clean}}
                                    col1.update_one(myquery, newvalues)
                            if title_auto.endswith('_') or title_auto.endswith('-') or title_auto.endswith(
                                    '|'):
                                title_auto = title_auto[:-1]
                                myquery = {"url": i2["url"]}
                                newvalues = {"$set": {"title_auto": title_auto}}
                                col1.update_one(myquery, newvalues)
                        if d == (c + 5):
                            break
                    if flag == 1:
                        myquery2 = {"url": i_host["url"]}
                        newvalues2 = {"$set": {"title_auto": title2.replace(i, '')}}
                        col1.update_one(myquery2, newvalues2)
                        title2_linshi = title2.replace(i, '')
                        if title2_linshi.endswith('_') or title2_linshi.endswith('-') or title2_linshi.endswith(
                                '|'):
                            myquery = {"url": i_host["url"]}
                            newvalues = {"$set": {"title_auto": title2_linshi[:-1]}}
                            col1.update_one(myquery, newvalues)

    title_auto_last = i_host['title_auto']
    return title_auto_last

# title_final抽取
def title_final_e(i, title_triple, title_auto_clean):
    title1 = i['title']
    title2 = title_auto_clean
    title3 = title_triple
    content = i['content']
    title_final = ''
    title1_judge = remove_punctuation(title1)
    title2_judge = remove_punctuation(title2)
    title3_judge = remove_punctuation(title3)
    # print(title1_judge)
    if len(title1_judge) > 2:
        title_final = title1
        title_final = remove_punctuation2(title_final).strip()
        myquery = {"url": i["url"]}
        newvalues = {"$set": {"title_final": title_final}}
        col1.update_one(myquery, newvalues)
    elif len(title2_judge) > 2:
        title_final = title2
        title_final = remove_punctuation2(title_final).strip()
        myquery = {"url": i["url"]}
        newvalues = {"$set": {"title_final": title_final}}
        col1.update_one(myquery, newvalues)
    elif len(title3_judge) > 2:
        title_final = title3
        title_final = remove_punctuation2(title_final).strip()
        myquery = {"url": i["url"]}
        newvalues = {"$set": {"title_final": title_final}}
        col1.update_one(myquery, newvalues)

    if content:
        text1 = content
        phraseList = HanLP.extractPhrase(text1, 1)
        if phraseList:
            title_final = phraseList[0]
            title_final = remove_punctuation2(title_final).strip()
    else:
        title_final = ''
    myquery = {"url": i["url"]}
    newvalues = {"$set": {"title_final": title_final}}
    col1.update_one(myquery, newvalues)

    return title_final




# TODO 抽取部分

def run():
    c = 0
    save = 0
    for i in col1.find():
        c += 1


        if c < 0:
            continue

        if 'title_final' not in i:
            print('c:', c)
            title_triple = ''
            save += 1
            print('save:', save)
            # TODO 抽取关键句部分
            content, title_list = key_sentence_e(i)
            if content:
                # TODO 抽取三元组部分
                title_triple = ltp_e(i, title_list)

            else:
                print('没有content')


            # TODO title清洗
            title1_original, title1_clean = title_cleaning(i, title_triple)


            # TODO title_auto清洗
            title_auto_clean = title_auto_cleaning(i, c, title1_clean)

            # TODO title_final抽取
            title_final = title_final_e(i, title_triple, title_auto_clean)

            print('清洗成功, url:', i['url'])


        else:
            pass


    return save


run()



