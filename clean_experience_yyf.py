'''
description: Used to extract expert experience 用于抽取专家经历
date: 2020-10-01
author: jtx
'''


import os
import pymongo
import datetime
import re
import logging
from logging.handlers import RotatingFileHandler
import requests
import json
from ltp_service import LtpService
import sys
from pathlib import Path

ltp_service = LtpService()


print(Path(__file__).name)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

# 获取当前文件名，建立log
dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

# print(kbp_path)
# print(dir_path)
# print(os.path.basename(dir_path))
# print(sys.argv)  # 输入参数列表
# print(sys.argv[0])  # 第0个就是这个python文件本身的路径（全路径）

def set_log():
    logging.basicConfig(level=logging.INFO)
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"CLEAN_EXPERIENCE_log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)
logger.info(dir_path)



client = pymongo.MongoClient('XXXXXXXXXXXXXXXX', 27017)
client.admin.authenticate("XXXXXXXXXXXXXXXX", "XXXXXXXXXXXXXXXXXXXX")
db1 = client.res_kb
# db.authenticate("res_kb", "res_kb123@", mechanism='MONGODB-CR')
# col = db.res_kb_technology_system

col1 = db1.res_kb_expert_lz_baike
db2 = client.yyf_db
col2 = db2.res_kb_process_expert_ai
col3 = db2.res_kb_expert_resource
col4 = db2.res_kb_expert_baike

db2 = client.industry_ic
col2_1 = db2.res_kb_expert_baike

# 清洗[]符号
def clean(value):
    list1 = ['[1]', '[2]', '[3]', '[4]', '[5]', '[6]', '[7]', '[8]', '[9]', '[10]']
    for i in list1:
        if i in value:
            value = value.replace(i, '')
            print('更新数据')
    return value

# 匹配日期
def match_date(str, num_update):
    flag_need_model = 0
    list_r = []
    s_group = ''
    # 一级
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[-/月.][至到～-]\d{4}[-/年.]\d{1,2}[-/月.])")
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[至到～-]\d{4}[-/年.]\d{1,2})")
    list_r.append(r"(\d{4}[-/年.]\d{4}[-/年.])")
    list_r.append(r"(\d{4}[-/年.][至到～-]\d{4}[-/年.])")
    # list_r.append(r"(\d{4}[-/年.]\d{4}[-/年.])")
    # list_r.append(r"(\d{4}[-/年.]\d{4})")
    # 二级
    list_r.append(r"(\d{4}[-/年.][至今起任，])")
    list_r.append(r"(\d{4}[-/年.][毕业于])")
    list_r.append(r"(\d{4}[-/年.][开始在])")
    list_r.append(r"(\d{4}[-/年.][至今起任，])")
    list_r.append(r"(\d{4}[至今起任，])")
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[-/月.][至今起任，])")
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[-/月.][毕业于])")
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[-/月.][开始在])")
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[-/月.][至今起任，])")
    list_r.append(r"(\d{4}\d{1,2}[-/月.][至今起任，])")
    # 三级 准确率最低一级 需要调用模型判断 三元组抽取模型
    list_r.append(r"(\d{4}[-/年.]\d{1,2}[-/月.])")
    list_r.append(r"(\d{4}[-/年.])")

    if '立即投入到自己的科研实践之中' in str or '蔡鹤皋(5张)蔡鹤皋(5张)1982年4月—1985年9月，担任哈尔滨工业大学机械工程系副教授' in str:
        print(1)

    for each_r in list_r:
        # 判断是否调用模型
        if each_r == r"(\d{4}[-/年.]\d{1,2}[-/月.])" or each_r == r"(\d{4}[-/年.]":
            flag_need_model = 1
        c = re.compile(each_r)
        s = c.search(str)
        if s:
            if flag_need_model != 1:
                s_group = s.group()
                # print(s_group)
                break
            else:
                # 条件一
                triple = ltp_service.ltp_extract_triple(each_r)
                if triple:
                    print(triple)
                    num_update += 1
                    print(num_update)

                # 条件二 可选
                flag_key_word_in_str = 0
                key_words = ['学位', '学士', '博士', '获得', '毕业于', '任']
                for key_word in key_words:
                    if key_word in str:
                        flag_key_word_in_str = 1

                if triple or (flag_key_word_in_str == 1):
                    # triple_linshi = ''
                    # for each2 in triple[0]:
                    #     triple_linshi += each2
                    # triple_linshi = triple_linshi.strip().replace(' ', '').replace('\n', '').replace('\r', '')
                    # print(triple_linshi)
                    s_group = s.group()
                    print(str)
                    print(triple)
                    break

    return s_group

# 分句
def split(sentence):
    list_experience = []
    num_update = 0
    # 1
    str1_after1 = re.sub(' +', '', sentence)
    str1_after2 = re.sub('\n+', '\n', str1_after1)
    # print(str1_after2)
    # 2
    sentence_list = re.split('[。；？！\n ]', str1_after2)
    logger.info('list_sentence:{}'.format(sentence_list))
    # 3
    for each_sentence in sentence_list:
        # print('each_sentence:', each_sentence)
        s_group = match_date(each_sentence, num_update)
        if s_group:
            each_sentence_real = each_sentence[each_sentence.rfind(s_group):]
            each_sentence_real = clean(each_sentence_real)
            list_experience.append(each_sentence_real)

    return list_experience, sentence_list

for num, i in enumerate(col2_1.find()):
    if num < 2664:
        print(num)
        continue

    flag_experience = 0
    list_experience = []
    sentence_list = []
    _id = i['_id']
    logger.info("num:{}, _id:{}".format(str(num), _id))
    tag= i['tag']
    for key, value in tag.items():
        # print(key)
        if '经历' in key or '履历' in key:
            # print(type(value))
            if type(value) == dict:
                # print(value)
                for key_in_value, value_in_value in value.items():
                    # print(type(value_in_value))
                    list_experience, sentence_list = split(value_in_value)
            else:
                list_experience, sentence_list = split(value)

            logger.info('list_experience:{}'.format(list_experience))

            # 更新数据
            # newvalues = {"$set": {"list_sentence_need_delete": sentence_list, 'list_experience':list_experience}}
            newvalues = {"$set": {'list_experience': list_experience}}
            col2_1.update_one({'_id':_id}, newvalues)
            logger.info('---------------更新成功-------------------------------')
            flag_experience = 1
    if flag_experience == 0:
        # newvalues = {"$set": {"list_sentence_need_delete": sentence_list, 'list_experience': list_experience}}
        newvalues = {"$set": {'list_experience': list_experience}}
        col2_1.update_one({'_id': _id}, newvalues)
        logger.info('-------------------经历数据为空---------------------------')






