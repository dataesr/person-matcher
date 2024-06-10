#!/usr/bin/env python
# coding: utf-8

import requests
import math
import json
import os
from datetime import datetime
from bson.objectid import ObjectId

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from project.server.main.utils import chunks, to_jsonl, to_json

from project.server.main.s3 import upload_object

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

header = {'Authorization':f"Basic {os.getenv('DATAESR_HEADER')}"}

def get_with_retry(url):
    s = requests.Session()
    s.headers.update(header)
    return requests_retry_session(session=s).get(url)
    
def dump_from_http():
    db = 'organizations'
    collection = 'scanr'
    url_base = "http://185.161.45.213/{0}/{1}".format(db, collection)
    nb_res = get_with_retry(url_base).json()['meta']['total']
    nb_pages = math.ceil(nb_res/500)
    print(nb_res, nb_pages)
    current_list = []
    for p in range(1, nb_pages + 1):
        print(p, end=',')
        url = url_base+"?max_results=500&page="+str(p)
        r = get_with_retry(url)
        current_list += r.json()['data']
    current_list2=[]
    for elem in current_list:
        if 'id' in elem:
            elem['id'] = elem['id'][0:450]
            if len(elem['id'])>450:
                 print(len(elem['id']), elem['id'])
        for field in ['_id', 'etag', 'created_at', 'modified_at']:
            if field in elem:
                del elem[field]
        current_list2.append(elem)
    os.system('rm -rf /upw_data/scanr/organizations.jsonl')
    to_jsonl(current_list2, '/upw_data/scanr/organizations.jsonl')
    os.system(f'cd /upw_data/scanr && rm -rf organizations.jsonl.gz && gzip -k organizations.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/organizations.jsonl.gz', destination='production/organizations.jsonl.gz')
