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

person_id_key = 'person'

# sed -e 's/\"prizes\": \[\(.*\)\}\], \"f/f/' persons2.json > persons.json &

def load_projects(args):
    df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz', lines=True)
    projects = df.to_dict(orient='records')
    df_orga = get_orga_data()
    os.system('rm -rf /upw_data/scanr/projects_denormalized.jsonl')
    denormalized_projects = []
    for p in projects:
        for part in p.get('participants'):
            part_id = part.get('structure')
            if part_id:
                denormalized_organization = get_orga(df_orga, part_id)
                part['structure'] = denormalized_organization
        text_to_autocomplete = []
        for lang in ['default', 'en', 'fr']:
            for k in ['label', 'acronym']:
                if isinstance(p.get(k), dict):
                    if isinstance(p[k].get(lang), str):
                        text_to_autocomplete.append(p[k][lang])
        text_to_autocomplete.append(p['id'])
        text_to_autocomplete = list(set(text_to_autocomplete))
        p['autocompleted'] = text_to_autocomplete
        p['autocompletedText'] = text_to_autocomplete
    to_jsonl(projects, '/upw_data/scanr/projects_denormalized.jsonl') 
    load_scanr_projects('/upw_data/scanr/projects_denormalized.jsonl', 'scanr-projects-20231211')    

def load_scanr_projects(scanr_output_file_denormalized, index_name):
    denormalized_file=scanr_output_file_denormalized
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-projects index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={denormalized_file} --output={es_host}{index_name} --type=data --limit 500 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)
    refresh_index(index_name)
