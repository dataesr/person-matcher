import requests
import math
import json
import os
from datetime import datetime
import pandas as pd
from bson.objectid import ObjectId

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.paysage import get_paysage_data, get_main_id_paysage, get_correspondance_paysage
from project.server.main.s3 import upload_object
from project.server.main.export_data_without_tunnel import get_with_retry
from project.server.main.paysage import get_correspondance_paysage
from project.server.main.utils import identifier_type
from project.server.main.logger import get_logger

logger = get_logger(__name__)

DATAESR_URL = os.getenv('DATAESR_URL')

def dump_rnsr_data(nb_per_page=500):
    logger.debug('### DUMP RNSR data')
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
        elem['isFrench'] = True
        if 'id' in elem:
            elem['id'] = elem['id'][0:450]
            if len(elem['id'])>450:
                 print(len(elem['id']), elem['id'])
        for field in ['_id', 'etag', 'created_at', 'modified_at']:
            if field in elem:
                del elem[field]
        #for g in ['institutions', 'parents', 'predecessors', 'relations', 'spinoff']:
        for x in elem.get('externalIds', []):
            if x.get('type') == 'rnsr':
                elem['id'] = x['id']
                break
        current_list2.append(elem)
    os.system('mkdir -p /upw_data/scanr/orga_ref')
    os.system(f'rm -rf /upw_data/scanr/orga_ref/rnsr.jsonl')
    to_jsonl(current_list2, f'/upw_data/scanr/orga_ref/rnsr.jsonl')
    os.system(f'cd /upw_data/scanr/orga_ref && rm -rf rnsr.jsonl.gz && gzip -k rnsr.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/orga_ref/rnsr.jsonl.gz', destination=f'production/rnsr.jsonl.gz')

def format_rnsr():
    logger.debug('formatting RNSR data')
    df = pd.read_json(f'/upw_data/scanr/orga_ref/rnsr.jsonl.gz', lines=True)
    corresp = get_correspondance_paysage()
    data = []
    for e in df.to_dict(orient='records'):
        for g in ['institutions', 'parents', 'predecessors', 'relations', 'spinoff']:
            if isinstance(e.get(g), list):
                for k in e[g]:
                    if isinstance(k, dict) and 'structure' in k:
                        #k['structure'] = get_main_id(k['structure'])
                        k['structure'] = get_main_id_paysage(k['structure'], corresp)
                        if identifier_type(k['structure']) not in ['rnsr', 'paysage', 'ror']:
                            logger.debug(f'tutelle identification to check {k}')
        data.append(e)
    os.system('cd /upw_data/scanr/orga_ref/ && rm -rf rnsr_formatted.jsonl')
    to_jsonl(data, f'/upw_data/scanr/orga_ref/rnsr_formatted.jsonl')
    return data
        
