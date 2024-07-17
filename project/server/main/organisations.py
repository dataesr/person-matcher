from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object, delete_object
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.denormalize_affiliations import get_orga, get_orga_data, get_projects_data, get_project, get_link_orga_projects, get_project_from_orga, get_main_address, compute_is_french 
from project.server.main.patents import get_patents_orga_dict, get_patent_from_orga
from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.elastic import reset_index_scanr, refresh_index
from project.server.main.scanr2 import get_publications_for_affiliation
from project.server.main.ods import get_awards, get_agreements
from project.server.main.export_data_without_tunnel import dump_from_http

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
import zipfile, io

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

def make_clean_html(html):
    soup = BeautifulSoup(html, 'lxml')
    text = soup.get_text().replace('\n',' ').replace('\t',' ').replace('  ', ' ')
    return text

def get_html_from_crawler(current_id, get_zip):
    for c in current_id.lower():
        if (c not in '0123456789abcdefghijklmnopqrstuvwxyz.'):
            return ''
    if get_zip:
        try:
            OC_FILES_URL = f"{os.getenv('OC_URL')}/files/{current_id}"
            os.system(f'rm -rf /upw_data/crawl/crawl_{current_id}')
            r = requests.get(OC_FILES_URL)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(f"/upw_data/crawl/crawl_{current_id}")
        except:
            logger.debug(f'no zip for {current_id}')
            return ''

    all_html=''
    f = []
    for (dirpath, dirnames, filenames) in os.walk(f'/upw_data/crawl/crawl_{current_id}/html'):
        for filename in filenames:
            f.append(f'{dirpath}/{filename}')
            current_html = open(f'{dirpath}/{filename}', 'r').read()
            all_html+=' '+make_clean_html(current_html)
    return all_html

def compute_reverse_relations(data):
    reverse_relation_fields = ['parents', 'institutions', 'relations', 'predecessors']
    reverse_relation={}
    for f in reverse_relation_fields:
        reverse_relation[f] = {}

    for e in data:
        current_id = e['id']
        for f in reverse_relation_fields:
            if isinstance(e.get(f), list) and e[f]:
                for reversed_elt in e[f]:
                    reversed_elt_id = reversed_elt.get('structure')
                    if reversed_elt_id:
                        if reversed_elt_id not in reverse_relation[f]:
                            reverse_relation[f][reversed_elt_id] = []
                        new_reverse = reversed_elt.copy()
                        new_reverse['structure'] = current_id
                        reverse_relation[f][reversed_elt_id].append(new_reverse)
    return reverse_relation

def load_orga(args):
    index_name = args.get('index_name')
    get_zip_from_crawler = args.get('get_zip_from_crawler', True)
    if args.get('export_from_source', False):
        dump_from_http()
    if args.get('reload_index_only', False) is False:
        df_orga = get_orga_data()
        df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz', lines=True)
        orga = df.to_dict(orient='records')
        reverse_relation = compute_reverse_relations(orga)
        map_proj_orga = get_link_orga_projects()
        map_patent_orga = get_patents_orga_dict()
        map_agreements = get_agreements()
        map_awards = get_awards()

        url_ai_descr = 'https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/misc/scanr_organizations_mistral_descriptions.json'
        df_ai_description = pd.read_json(url_ai_desc)

        os.system('rm -rf /upw_data/scanr/organizations/organizations_denormalized.jsonl')
        for ix, p in enumerate(orga):
            new_p = p.copy()
            current_id = new_p['id']
            
            new_p['has_ai_description'] = False
            df_ai_description_tmp = df_ai_description[df_ai_description.index==current_id]
            if len(df_ai_description_tmp)>0:
                ai_desc_data = df_ai_description_tmp.to_dict(orient='records')[0]
                ai_description = {
                   'creation_date': ai_desc_data['creation_date'],
                  'model': ai_desc_data['mistral_model'],
                  'description': ai_desc_data['data']['description_mistral']
                }
                new_p['ai_description'] = ai_description
                new_p['has_ai_description'] = True

            reasons_scanr = []
            if new_p.get('status') == 'active' and isinstance(new_p.get('links'), list):
                web_content = get_html_from_crawler(current_id = current_id, get_zip = get_zip_from_crawler)
                if isinstance(web_content, str) and len(web_content)>10:
                    new_p['web_content'] = web_content
            mainAddress = get_main_address(p.get('address'))
            new_is_french = compute_is_french(current_id, mainAddress)
            if new_is_french != p.get('isFrench'):
                logger.debug(f"new isFrench for {current_id} {p['isFrench']} ==> {new_is_french}")
                new_p['isFrench'] = new_is_french
            for f in ['parents', 'institutions', 'relations']:
                if current_id in reverse_relation[f]:
                    new_p[f'{f[0:-1]}Of'] = reverse_relation[f][current_id]
                    logger.debug(f'{f[0:-1]}Of {len(reverse_relation[f][current_id])}')
            logger.debug(f"denormalize orga {p['id']} ({ix}/{len(orga)})")
            publications_data = get_publications_for_affiliation(p['id'])
            new_p['publications'] = publications_data['publications']
            new_p['publicationsCount'] = publications_data['count']
            new_p['projects'] = get_project_from_orga(map_proj_orga, p['id'])
            new_p['projectsCount'] = len(new_p['projects'])
            new_p['patents'] = get_patent_from_orga(map_patent_orga, p['id'])
            new_p['patentsCount'] = len(new_p['patents'])
            nb_publis = new_p['publicationsCount']
            if nb_publis > 0:
                reasons_scanr.append('publication')
            nb_projects = new_p['projectsCount']
            if nb_projects > 0:
                reasons_scanr.append('project')
            nb_patents = new_p['patentsCount']
            if nb_patents > 0:
                reasons_scanr.append('patent')
            logger.debug(f'nb_publis = {nb_publis}, nb_projects={nb_projects}, nb_patents={nb_patents}')
            nb_relationships = 0
            for f in ['institutions', 'predecessors', 'relations', 'parents', 'parentOf', 'institutionOf', 'relationOf', 'predecessorOf']:
                if not isinstance(new_p.get(f), list):
                    continue
                for ix, e in enumerate(new_p[f]):
                    if isinstance(e, dict) and isinstance(e.get('structure'), str):
                        nb_relationships += 1
                        reasons_scanr.append('relationship')
                        current_id = e.get('structure')
                        new_p[f][ix]['denormalized'] = get_orga(df_orga, current_id)
                if f not in ['predecessors']:
                    new_p[f] = [org for org in new_p[f] if org.get('denormalized', {}).get('status', '') == 'active']
            if new_p.get('spinoffs'):
                del new_p['spinoffs']
            if current_id in map_awards:
                new_p['awards'] = map_awards[current_id]
                reasons_scanr.append('awards')
            if current_id in map_agreements:
                new_p['agreements'] = map_agreements[current_id]
                reasons_scanr.append('agreements')
            #if isinstance(p.get('badges'), list) and len(p['badges']) > 0:
            #    new_p['badges'] = p['badges']
            #    nb_badges = len(p['badges'])
            #    reasons_scanr.append('badge')
            text_to_autocomplete = []
            for lang in ['default', 'en', 'fr']:
                for k in ['label', 'acronym']:
                    if isinstance(new_p.get(k), dict):
                        if isinstance(new_p[k].get(lang), str):
                            text_to_autocomplete.append(new_p[k][lang])
            if new_p.get('alias'):
                text_to_autocomplete += new_p['alias']
            for ext in new_p.get('externalIds', []):
                if isinstance(ext.get('id'), str):
                    text_to_autocomplete.append(ext['id'])
                if ext.get('type') == 'rnsr':
                    reasons_scanr.append('rnsr')
            reasons_scanr = list(set(reasons_scanr))
            reasons_scanr.sort()
            new_p['reasons_scanr'] = reasons_scanr
            if len(reasons_scanr) == 0:
                logger.debug(f"ignore {p['id']} - no publi / project / patent / relationship / badge")
                continue
            text_to_autocomplete = list(set(text_to_autocomplete))
            new_p['autocompleted'] = text_to_autocomplete
            new_p['autocompletedText'] = text_to_autocomplete
            to_jsonl([new_p], '/upw_data/scanr/organizations/organizations_denormalized.jsonl')
    load_scanr_orga('/upw_data/scanr/organizations/organizations_denormalized.jsonl', index_name)
    os.system(f'cd {MOUNTED_VOLUME}scanr/organizations && rm -rf organizations_denormalized.jsonl.gz && gzip -k organizations_denormalized.jsonl')
    upload_object(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/organizations/organizations_denormalized.jsonl.gz', destination='production/organizations_denormalized.jsonl.gz')

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


def launch_crawl():
    df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz', lines=True)
    orga = df.to_dict(orient='records')

    structure_list_to_crawl = []
    for org in orga:
        if org.get('status') != 'active':
            continue
        if not isinstance(org.get('links'), list):
            continue
        for link in org['links']:
            if link.get('type')=='main' and isinstance(link.get('url'), str):
                new_elt = {'carbon_footprint': {'enabled': False},
                'technologies_and_trackers': {'enabled': True},
                'lighthouse': {'enabled': True},
                'depth': 2,
                'limit': 10,
                'tags': ['scanr'],
                'identifiers': [org['id']],
                'url': link['url']}
                structure_list_to_crawl.append(new_elt)

    OC_URL = os.getenv('OC_URL')
    for e in structure_list_to_crawl:
        r = requests.post(OC_URL, json=e).json()
        #print(r)            
