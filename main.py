#!/usr/bin/python
# -*- coding: utf-8 -*-
#####################################
# File name : main.py
# Create date : 2019-08-21 10:23
# Modified date : 2019-08-21 11:16
# Author : DARREN
# Describe : not set
# Email : lzygzh@126.com
#####################################
from __future__ import division
from __future__ import print_function

from collect_news import CollectNews
from history_hot import HistoryHot

def run_collect_news():
    handler = CollectNews()
    handler.collect()
    handler.collect2()
    handler.collect3()
    handler.collect4()

def run_history_hot():
    handler = HistoryHot()
    handler.process_main_2004()

def run():
    run_history_hot()
    run_collect_news()

run()
