#!/usr/bin/env python

import configparser
import logging
import logging.config
import sys

from datetime import date
from elasticsearch import Elasticsearch

logging.config.fileConfig('index-config.ini')

config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

index_name = config['elasticsearch']['index']

today = date.today()
today_string = today.strftime("%Y%m%d")

if (es.ping()):
    print("ping!")

if es.indices.exists(index_name):
    logging.info("Dropping index : %s" % (index_name))
    es.indices.delete(index_name)
else:
    logging.info("Index %s does not exist" % (index_name))
