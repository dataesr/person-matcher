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
from project.server.main.paysage import get_paysage_data, get_status_from_siren
from project.server.main.s3 import upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

DATAESR_URL = os.getenv('DATAESR_URL')

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
    
def dump_from_http(db, nb_per_page = 500):
    df_paysage_struct, df_siren, df_ror = get_paysage_data()
    collection = 'scanr'
    url_base = f"{DATAESR_URL}/{db}/{collection}"
    nb_res = get_with_retry(url_base).json()['meta']['total']
    nb_pages = math.ceil(nb_res/nb_per_page)
    print(nb_res, nb_pages)
    current_list = []
    for p in range(1, nb_pages + 1):
        print(p, end=',')
        url = url_base+f"?max_results={nb_per_page}&page="+str(p)
        try:
            r = get_with_retry(url)
            current_list += r.json()['data']
        except:
            logger.debug(f'error with {url}, skip that page')
    current_list2=[]
    for elem in current_list:
        if 'id' in elem:
            elem['id'] = elem['id'][0:450]
            if len(elem['id'])>450:
                 print(len(elem['id']), elem['id'])
        for field in ['_id', 'etag', 'created_at', 'modified_at']:
            if field in elem:
                del elem[field]
        siren = None
        for ext in elem.get('externalIds', []):
            if ext.get('type') == 'sirene':
                siren = ext['id']
                break
        if siren:
            paysage_info = get_status_from_siren(siren, df_paysage_struct, df_siren, df_ror)
            if paysage_info and paysage_info.get('status') != elem.get('status'):
                elem.update(paysage_info)
                logger.debug(f'updating siren {siren} with paysage info {paysage_info}')
        current_list2.append(elem)
    os.system(f'rm -rf /upw_data/scanr/{db}.jsonl')
    to_jsonl(current_list2, f'/upw_data/scanr/{db}.jsonl')
    os.system(f'cd /upw_data/scanr && rm -rf {db}.jsonl.gz && gzip -k {db}.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/{db}.jsonl.gz', destination=f'production/{db}.jsonl.gz')


def dump_rnsr_data(nb_per_page=500, uai2siren={}):
    db = 'organizations'
    collection = 'scanr'
    url_base = f'{DATAESR_URL}/{db}/{collection}?where=' + '{"externalIds.type":"rnsr"}'
    nb_res = get_with_retry(url_base).json()['meta']['total']
    nb_pages = math.ceil(nb_res/nb_per_page)
    logger.debug(f'getting RNSR data {nb_res} elts over {nb_pages} pages')
    current_list = []
    for p in range(1, nb_pages + 1):
        print(p, end=',')
        url = url_base+f"&max_results={nb_per_page}&page="+str(p)
        try:
            r = get_with_retry(url)
            current_list += r.json()['data']
        except:
            logger.debug(f'error with {url}, skip that page')
    current_list2=[]
    for elem in current_list:
        if 'id' in elem:
            elem['id'] = elem['id'][0:450]
            if len(elem['id'])>450:
                 print(len(elem['id']), elem['id'])
        for field in ['_id', 'etag', 'created_at', 'modified_at']:
            if field in elem:
                del elem[field]
        for k in elem.get('institutions', []):
            if isinstance(k.get('structure'), str) and k['structure'] in uai2siren:
                logger.debug(f"replace UAI {k['structure']} with {uai2siren[k['structure']]}")
                k['structure'] = uai2siren[k['structure']]
        current_list2.append(elem)
    os.system('mkdir -p /upw_data/scanr/orga_ref')
    os.system(f'rm -rf /upw_data/scanr/orga_ref/rnsr.jsonl')
    to_jsonl(current_list2, f'/upw_data/scanr/orga_ref/rnsr.jsonl')
    os.system(f'cd /upw_data/scanr/orga_ref && rm -rf rnsr.jsonl.gz && gzip -k rnsr.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/orga_ref/rnsr.jsonl.gz', destination=f'production/rnsr.jsonl.gz')
