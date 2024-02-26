from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object, delete_object
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.elastic import reset_index_scanr, refresh_index

import pysftp
import requests
from bs4 import BeautifulSoup
import os
import json
import pymongo
import pandas as pd
from retry import retry
from dateutil import parser
from urllib import parse

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

def load_geo(args):
    index_name = args.get('index_name')
    r = requests.get('http://185.161.45.213/organizations/_distinct/scanr/address.localisationSuggestions', headers={'Authorization': f"Basic {os.getenv('DATAESR_HEADER')}"}).json()
    data = [ {'autocompleted': [loc]} for loc in r['values']]
    geo_file = '/upw_data/scanr/localisations.jsonl'
    os.system(f'rm -rf {geo_file}')
    to_jsonl(data, geo_file)
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-localisations index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={geo_file} --output={es_host}{index_name} --type=data --limit 500 " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)
    refresh_index(index_name)
