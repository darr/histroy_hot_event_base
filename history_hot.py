#!/usr/bin/python
# -*- coding: utf-8 -*-
#####################################
# File name : history_hot.py
# Create date : 2019-08-21 11:14
# Modified date : 2019-08-21 11:16
# Author : DARREN
# Describe : not set
# Email : lzygzh@126.com
#####################################
from __future__ import division
from __future__ import print_function

import pymongo
import chardet
import urllib.request
from urllib.parse import quote_plus
import json
import datetime
from lxml import etree


class HistoryHot:
    def __init__(self):
        self.start_date = '2010-01-01'
        self.end_date = '2019-04-18'
        self.conn = pymongo.MongoClient()
        return

    '''获取搜索页'''
    def get_html(self, url):
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/600.5.17 (KHTML, like Gecko) Version/8.0.5 Safari/600.5.17"}
        req = urllib.request.Request(url, headers=headers)
        html = urllib.request.urlopen(req,timeout=5).read().decode('utf-8')
        data = '{"conf"' + html.split('= {"conf"')[-1].split('}};\n')[0]
        if 'ext' in data and 'typeof' not in data:
            try:
                data = data.replace('};', '}')
                data = json.loads(data)
            except Exception as e:
                print(e)
                data = {}
        else:
            data = {}

        return data


    '''获取搜索页'''
    def get_html_pre(self, url):
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/600.5.17 (KHTML, like Gecko) Version/8.0.5 Safari/600.5.17"}
        req = urllib.request.Request(url, headers=headers)
        html = urllib.request.urlopen(req, timeout=5).read()
        code = chardet.detect(html)['encoding']
        try:
            html = html.decode(code)
        except:
            html = html.decode('gbk')
        return html


    '''获取每日的热点新闻'''
    def collect_history_hot(self, date):
        top_num = 100
        # 总排行
        url_dict = {
        "zong_dj" : "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=www_all&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "zong_pl": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=qbpdpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "zong_fx": "http://top.collection.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzf_qz&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "zong_sp": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=video_news_all_by_vv&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "zong_tp": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=total_slide_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 国内新闻
        "guonei_dj": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=china&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "guonei_pl": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=gnxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "guonei_fx": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfgnxw&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 国际新闻
        "guoji_dj": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=world&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "guoji_pl": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=gjxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "guoji_fx": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfgwxw&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 社会新闻
        "shehui_dj": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=society&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "shehui_pl": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=shxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "shehui_fx": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfshxw&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 体育新闻
        "tiyu_dj": "http://top.sports.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=all&top_time={0}&top_show_num={1}&top_order=ASC".format(date, top_num),
        "tiyu_pl": "http://top.sports.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=tyxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "tiyu_fx": "http://top.sports.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfty&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 财经新闻
        "caijing_dj": "http://top.finance.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=all&top_time={0}&top_show_num={1}&top_order=ASC".format(date, top_num),
        "caijing_pl": "http://top.finance.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=cjxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "caijing_fx": "http://top.finance.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfcj&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 娱乐新闻
        "yule_dj": "http://top.ent.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=all&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "yule_pl": "http://top.ent.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=ylxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "yule_fx": "http://top.ent.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfyl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 科技新闻
        "keji_dj": "http://top.tech.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=all&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "keji_pl": "http://top.tech.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=kjxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "keji_fx": "http://top.tech.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfkj&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),

        # 军事新闻
        "junshi_dj":"http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=all&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "junshi_pl": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=jsxwpl&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num),
        "junshi_fx": "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=wbrmzfjsxw&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)}

        if date > '20140509':
            url_dict["guonei_dj"] = "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=news_china_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(
            date, top_num)
            url_dict["guoji_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=news_world_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(
                date, top_num)
            url_dict["shehui_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=news_society_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)
            url_dict["tiyu_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=sports_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)
            url_dict["yule_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=ent_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)
            url_dict["keji_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=tech_news_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)
            url_dict["junshi_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=news_mil_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)
            url_dict["caijing_dj"]= "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=finance_0&top_time={0}&top_show_num={1}&top_order=DESC".format(date, top_num)

        if date > '20150415':
            url_dict["zong_dj"] = "http://top.news.sina.com.cn/ws/GetTopDataList.php?top_type=day&top_cat=www_www_all_suda_suda&top_time={0}&top_show_num={1}&top_order=DESC".format(
                date, top_num)

        for topic, url in url_dict.items():
            if date < '20171015':
                continue
            try:
                html = self.get_html(url)
                if not html:
                    continue
                records = html['data']
                print(date, topic, len(records))
                data = {}
                data['date'] = date
                data['topic'] = topic
                data['url'] = url
                data['data'] = records
                self.conn['history_hot']['data'].insert(data)
            except Exception as e:
                print(e)

        return


    '''采集主函数'''
    def process_main_2010(self):
        dates = self.collect_dates(self.start_date, self.end_date)
        print(dates)
        print(len(dates))
        for date in dates:
            self.collect_history_hot(date)
        return

    '''采集2004年到2009年的数据'''
    def process_main_2004(self):
        start_date = '2004-07-05'
        end_date = '2009-12-31'
        date_list = self.collect_dates(start_date, end_date)
        for date in date_list:
            url = "http://news.sina.com.cn/hotnews/{0}.shtml".format(date.replace('-', ''))
            try:
                html = self.get_html_pre(url)
                print(url)
                if date < '20050425':
                    self.parser_1(html, date)
                elif date < '20071218':
                    self.parser_2(html, date)
                else:
                    self.parser_3(html, date)
            except Exception as e:
                print(e)


    '''解析第一个版本http://news.sina.com.cn/hotnews/20050424.shtml'''
    def parser_1(self, html, date):
        datas = []
        htmls = html.split('<table border=0 cellpadding=0 cellspacing=0>')[1:-1]
        def get_pairs(html):
            datas = []
            selector = etree.HTML(html)
            titles = selector.xpath('//td/a/text()')
            urls = selector.xpath('//td/a/@href')
            for url, title in zip(urls, titles):
                data = {}
                data['url'] = url
                data['title'] = title
                datas.append(data)
            return datas

        data_guonei = {'topic': 'guonei_dj', 'create_date':date}
        data_guoji = {'topic': 'guoji_dj', 'create_date':date}
        data_tiyu = {'topic': 'tiyu_dj', 'create_date':date}
        data_keji = {'topic': 'keji_dj', 'create_date':date}
        data_caijing = {'topic': 'caijing_dj', 'create_date':date}
        data_yule = {'topic': 'yule_dj', 'create_date':date}
        data_shehui = {'topic': 'shehui_dj', 'create_date':date}

        data_guonei['data'] = get_pairs(htmls[0])
        data_guoji['data'] = get_pairs(htmls[1])
        data_tiyu['data'] = get_pairs(htmls[2])
        data_keji['data'] = get_pairs(htmls[3])
        data_caijing['data'] = get_pairs(htmls[4])
        data_yule['data'] = get_pairs(htmls[5])
        data_shehui['data'] = get_pairs(htmls[6])

        datas.append(data_caijing)
        datas.append(data_yule)
        datas.append(data_shehui)
        datas.append(data_keji)
        datas.append(data_tiyu)
        datas.append(data_guoji)
        datas.append(data_guonei)
        self.insert_database(datas)
        return




    '''解析第二个版本http://news.sina.com.cn/hotnews/20050425.shtml'''
    def parser_2(self, html, date):
        datas = []
        htmls = html.split('<table cellspacing=01 bgcolor=#E8E8E8>')[1:]
        htmls = [html.split("<table width=746 cellspacing=0 style='margin:1px 0 1px 0' bgcolor=#F1F1F1>")[0] for html in htmls]
        def get_pairs(html):
            datas = []
            selector = etree.HTML(html)
            titles = selector.xpath('//td/span/a/text()')
            urls = selector.xpath('//td/span/a/@href')
            for url, title in zip(urls, titles):
                data = {}
                data['url'] = url
                data['title'] = title
                datas.append(data)
            return datas
        data_guonei = {'topic': 'guonei_dj', 'create_date': date}
        data_guoji = {'topic': 'guoji_dj', 'create_date': date}
        data_tiyu = {'topic': 'tiyu_dj', 'create_date': date}
        data_junshi = {'topic': 'junshi_dj', 'create_date': date}
        data_keji = {'topic': 'keji_dj', 'create_date': date}
        data_caijing = {'topic': 'caijing_dj', 'create_date': date}
        data_yule = {'topic': 'yule_dj', 'create_date': date}

        data_guonei['data'] = get_pairs(htmls[0])
        data_guoji['data'] = get_pairs(htmls[1])
        data_tiyu['data'] = get_pairs(htmls[2])
        data_junshi['data'] = get_pairs(htmls[3])
        data_keji['data'] = get_pairs(htmls[4])
        data_caijing['data'] = get_pairs(htmls[5])
        data_yule['data'] = get_pairs(htmls[6])

        datas.append(data_caijing)
        datas.append(data_yule)
        datas.append(data_keji)
        datas.append(data_tiyu)
        datas.append(data_guoji)
        datas.append(data_guonei)
        datas.append(data_junshi)

        self.insert_database(datas)

        return



    '''解析第二个版本http://news.sina.com.cn/hotnews/20050425.shtml'''
    def parser_3(self, html, date):
        datas = []
        htmls = html.split('<table cellspacing="0">')[1:-1]
        def get_pairs(html):
            datas = []
            selector = etree.HTML(html)
            titles = selector.xpath('//td[@class="ConsTi"]/a/text()')
            urls = selector.xpath('//td[@class="ConsTi"]/a/@href')
            for url, title in zip(urls, titles):
                data = {}
                data['url'] = url
                data['title'] = title
                datas.append(data)
            return datas

        data_zong = {'topic': 'zong_dj', 'create_date':date}
        data_guonei = {'topic': 'guonei_dj', 'create_date':date}
        data_guoji = {'topic': 'guoji_dj', 'create_date':date}
        data_tiyu = {'topic': 'tiyu_dj', 'create_date':date}
        data_keji = {'topic': 'keji_dj', 'create_date':date}
        data_caijing = {'topic': 'caijing_dj', 'create_date':date}
        data_yule = {'topic': 'yule_dj', 'create_date':date}
        data_shehui = {'topic': 'shehui_dj', 'create_date':date}
        data_junshi = {'topic': 'junshi_dj', 'create_date':date}

        data_zong['data'] = get_pairs(htmls[0])
        data_guonei['data'] = get_pairs(htmls[1])
        data_guoji['data'] = get_pairs(htmls[2])
        data_shehui['data'] = get_pairs(htmls[3])
        data_tiyu['data'] = get_pairs(htmls[4])
        data_keji['data'] = get_pairs(htmls[5])
        data_caijing['data'] = get_pairs(htmls[6])
        data_yule['data'] = get_pairs(htmls[7])
        data_junshi['data'] = get_pairs(htmls[8])

        datas.append(data_caijing)
        datas.append(data_yule)
        datas.append(data_shehui)
        datas.append(data_keji)
        datas.append(data_tiyu)
        datas.append(data_guoji)
        datas.append(data_guonei)
        datas.append(data_junshi)
        datas.append(data_zong)

        self.insert_database(datas)
        return


    '''插入数据库'''
    def insert_database(self, datas):
        for data in datas:
            self.conn['history_hot']['data_2004'].insert(data)


    '''获取某个时间段中的所有日期'''
    def collect_dates(self, start_date, end_date):
        date_list = []
        begin_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        while begin_date <= end_date:
            date_str = begin_date.strftime("%Y-%m-%d")
            date_list.append(date_str)
            begin_date += datetime.timedelta(days=1)
        return [i.replace('-','') for i in date_list]


if __name__ == '__main__':
    handler = HistoryHot()
    handler.process_main_2004()
