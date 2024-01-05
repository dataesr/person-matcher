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

def load_project():
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

def load_orga(args):
    if args.get('reload_index_only', False) is False:
        df_orga = get_orga_data()
        df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz', lines=True)
        orga = df.to_dict(orient='records')
        map_proj_orga =get_link_orga_projects()
        os.system('rm -rf /upw_data/scanr/organizations/organizations_denormalized.jsonl')
        for ix, p in enumerate(orga):
            new_p = p.copy()
            logger.debug(f"denormalize orga {p['id']} ({ix}/{len(orga)})")
            new_p['publications'] = get_publications_for_affiliation(p['id'])
            new_p['projects'] = get_project_from_orga(map_proj_orga, p['id'])
            nb_publis = len(new_p['publications'])
            nb_projects = len(new_p['projects'])
            logger.debug(f'nb_publis = {nb_publis}, nb_projects={nb_projects}')
            for f in ['institutions', 'predecessors', 'spinoffs', 'relations']:
                if not isinstance(new_p[f], list):
                    continue
                for ix, e in enumerate(new_p[f]):
                    if isinstance(e, dict) and isinstance(e.get('structure'), str):
                        current_id = e.get('structure')
                        new_p[f][ix]['denormalized'] = get_orga(df_orga, current_id)
            to_jsonl([new_p], '/upw_data/scanr/organizations/organizations_denormalized.jsonl')
    load_scanr_orga('/upw_data/scanr/organizations/organizations_denormalized.jsonl', 'scanr-organizations-20231211')

def load_scanr_orga(scanr_output_file_denormalized, index_name):
    denormalized_file=scanr_output_file_denormalized.split('/')[-1]
    scanr_output_dir_denormalized = '/'.join(scanr_output_file_denormalized.split('/')[0:-1])
    logger.debug('splitting files in 90MB chunks')
    os.system(f'cd {scanr_output_dir_denormalized} && rm -rf x* && split --line-bytes=90MB {denormalized_file}')
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    files_to_load = [k for k in os.listdir(scanr_output_dir_denormalized) if k[0:1]=='x']
    nb_files_to_load = len(files_to_load)
    reset_index_scanr(index=index_name)
    for ix, current_file in enumerate(files_to_load):
        logger.debug(f'loading scanr-organizations index {current_file} ({ix+1}/{nb_files_to_load})')
        elasticimport = f"elasticdump --input={scanr_output_dir_denormalized}/{current_file} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
        os.system(elasticimport)
    refresh_index(index_name)
