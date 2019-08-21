#!/usr/bin/python
# -*- coding: utf-8 -*-
#####################################
# File name : collect_news.py
# Create date : 2019-08-21 11:10
# Modified date : 2019-08-21 11:11
# Author : DARREN
# Describe : not set
# Email : lzygzh@126.com
#####################################
from __future__ import division
from __future__ import print_function

import os
import pymongo

class CollectNews:
    def __init__(self):
        self.conn = pymongo.MongoClient()
        return

    def collect(self):
        count = 0
        indx = 0
        for item in self.conn['history_hot']['data'].find():
            record = {}
            record['date'] = item['date']
            record['topic'] = item['topic']
            datas = item['data']
            datas_new = []
            for data in datas:
                data_ = {}
                data_['create_date'] = data['create_date'].replace('-','')
                data_['top_date'] = data['top_time']
                data_['comment_url'] = data['comment_url']
                data_['url'] = data['url']
                data_['author'] = data['author']
                data_['create_time'] = data["create_time"]
                data_['title'] = data["title"]
                top_year = int(data_['top_date'][:4])
                create_year = int(data_['create_date'][:4])
                if top_year > create_year + 1:
                    continue
                datas_new.append(data_)
                count += 1
            indx += 1
            print(indx, count)
            record['data'] = datas_new
            self.conn['history_hot']['final'].insert(record)
        return

    def collect2(self):
        count = 0
        indx = 0
        for item in self.conn['history_hot']['data_2004'].find():
            record = {}
            record['date'] = item['create_date']
            record['topic'] = item['topic']
            datas = item['data']
            datas_new = []
            for data in datas:
                data_ = {}
                data_['create_date'] = item['create_date']
                data_['top_date'] = item['create_date']
                data_['comment_url'] = ""
                data_['url'] = data['url']
                data_['author'] = ""
                data_['create_time'] = "00:00"
                data_['title'] = data["title"]
                datas_new.append(data_)
                count += 1
            indx += 1
            print(indx, count)
            record['data'] = datas_new
            self.conn['history_hot']['final'].insert(record)

        return

    def collect3(self):
        count = 0
        indx = 0
        for item in self.conn['history_hot']['final'].find():
            record = {}
            record['date'] = item['date']
            record['topic'] = item['topic']
            datas = item['data']
            try:
                titles = list(set([i['title']+'####'+i['url'] for i in datas]))
                record['titles'] = titles
                self.conn['history_hot']['titles'].insert(record)
                count += 1
                print(count)
            except Exception as e:
                print(e)


    def collect4(self):
        count = 0
        for topic_id in ['guonei_dj', 'guoji_dj']:
            for item in self.conn['history_hot']['titles'].find({'topic':topic_id}):
                self.conn['history_hot']['hot_titles'].insert(item)
                count += 1
                print(count)


if __name__ == '__main__':
    handler = CollectNews()
    handler.collect4()
