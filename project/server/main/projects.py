from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object, delete_object
from project.server.main.utils import chunks, to_jsonl, to_json, get_co_occurences
from project.server.main.s3 import upload_object
from project.server.main.denormalize_affiliations import get_orga, get_orga_data, get_projects_data, get_project, get_link_orga_projects, get_project_from_orga 
from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.elastic import reset_index_scanr, refresh_index
from project.server.main.scanr2 import get_publications_for_project, get_domains_from_publications
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

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

person_id_key = 'person'

# sed -e 's/\"prizes\": \[\(.*\)\}\], \"f/f/' persons2.json > persons.json &

def get_phc_duplicates(df):
    df_phc = df[df.type=='Partenariat Hubert Curien']
    to_del = []
    kb = {}
    all_p = {}
    for row in df_phc.itertuples():
        key = normalize(row.label['default'] +';' + str(int(row.year)))
        if key not in kb:
            kb[key] = [row.id]
            all_p[key] = []
        else:
            to_del.append(row.id)
        all_p[key].append(row.id)
    return list(set(to_del))

def load_projects(args):
    index_name = args.get('index_name')
    if args.get('export_from_source', True):
        dump_from_http('projects')
    if args.get('reload_index_only', False) is False:
        df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz', lines=True)
        phc_duplicates = get_phc_duplicates(df)
        projects = [p for p in df.to_dict(orient='records') if p['id'] not in phc_duplicates]
        df_orga = get_orga_data()
        os.system('rm -rf /upw_data/scanr/projects_denormalized.jsonl')
        for ix, p in enumerate(projects):
            denormalized_affiliations = []
            for part in p.get('participants'):
                part_id = part.get('structure')
                if part_id:
                    denormalized_organization = get_orga(df_orga, part_id)
                    part['structure'] = denormalized_organization
                    denormalized_affiliations.append(denormalized_organization)
            co_countries = get_co_occurences(denormalized_affiliations, 'country')
            if co_countries:
                projects[ix]['co_countries'] = co_countries
            structures_to_combine = [a for a in denormalized_affiliations if (('Structure de recherche' in a.get('kind', [])) and (a.get('status') == 'active'))]
            co_structures = get_co_occurences(structures_to_combine, 'id_name')
            if co_structures:
                projects[ix]['co_structures'] = co_structures
            institutions_to_combine = [a for a in denormalized_affiliations if (('Structure de recherche' not in a.get('kind', [])) and (a.get('status') == 'active'))]
            co_institutions = get_co_occurences(institutions_to_combine, 'id_name')
            if co_institutions:
                projects[ix]['co_institutions'] = co_institutions
            text_to_autocomplete = []
            for lang in ['default', 'en', 'fr']:
                for k in ['label', 'acronym']:
                    if isinstance(p.get(k), dict):
                        if isinstance(p[k].get(lang), str):
                            text_to_autocomplete.append(p[k][lang])
            publications_data = get_publications_for_project(p['id'])
            publis_to_expose = []
            for pub in publications_data['publications']:
                simple_publi = {}
                for f in ['id', 'projects', 'title', 'affiliations']:
                    if pub.get(f):
                        simple_publi[f] = pub[f]
                if simple_publi:
                    publis_to_expose.append(simple_publi)
            projects[ix]['publications'] = publis_to_expose
            projects[ix]['publicationsCount'] = publications_data['count']
            domains_infos = get_domains_from_publications(publications_data['publications'])
            projects[ix].update(domains_infos)
            logger.debug(f"{projects[ix]['publicationsCount']} publications retrieved for project {p['id']}")
            text_to_autocomplete.append(p['id'])
            text_to_autocomplete = list(set(text_to_autocomplete))
            projects[ix]['autocompleted'] = text_to_autocomplete
            projects[ix]['autocompletedText'] = text_to_autocomplete
        to_jsonl(projects, '/upw_data/scanr/projects_denormalized.jsonl') 
    os.system(f'cd {MOUNTED_VOLUME}scanr && rm -rf projects_denormalized.jsonl.gz && gzip -k projects_denormalized.jsonl')
    upload_object(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/projects_denormalized.jsonl.gz', destination='production/projects_denormalized.jsonl.gz')
    load_scanr_projects('/upw_data/scanr/projects_denormalized.jsonl', index_name) 

def load_scanr_projects(scanr_output_file_denormalized, index_name):
    denormalized_file=scanr_output_file_denormalized
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-projects index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={denormalized_file} --output={es_host}{index_name} --type=data --limit 50 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)
    refresh_index(index_name)
