from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object, delete_object
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.denormalize_affiliations import get_orga, get_orga_data, get_projects_data, get_project, get_link_orga_projects, get_project_from_orga 
from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.elastic import reset_index_scanr, refresh_index
from project.server.main.scanr2 import get_publications_for_affiliation

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

def load_patents(args):
    download_object(container='patstat', filename=f'fam_final_json.jsonl', out=f'{MOUNTED_VOLUME}/fam_final_json.jsonl')
    df = pd.read_json(f'{MOUNTED_VOLUME}/fam_final_json.jsonl', lines=True, chunksize=10000)
    df_orga = get_orga_data()
    os.system('rm -rf /upw_data/scanr/patents_denormalized.jsonl')
    for c in df:
        patents = c.to_dict(orient='records')
        denormalized_patents = []
        for p in patents:
            new_affiliations = []
            for aff_id in p.get('affiliations', []):
                denormalized_organization = get_orga(df_orga, aff_id)
                new_affiliations.append(denormalized_organization)
            p['affiliations'] = new_affiliations
        logger.debug(f'appending new denormalized data in patents')
        to_jsonl(patents, '/upw_data/scanr/patents_denormalized.jsonl') 
    load_scanr_patents('/upw_data/scanr/patents_denormalized.jsonl', 'scanr-patents-20231211')    

def load_scanr_patents(scanr_output_file_denormalized, index_name):
    denormalized_file=scanr_output_file_denormalized
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-patents index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={denormalized_file} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)
    refresh_index(index_name)
