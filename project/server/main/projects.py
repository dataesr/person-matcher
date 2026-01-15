from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object, delete_object
from project.server.main.utils import chunks, to_jsonl, to_json, get_co_occurences, save_to_mongo_publi_indexes
from project.server.main.s3 import upload_object
from project.server.main.denormalize_affiliations import get_orga, get_orga_map, get_projects_data, get_project, get_link_orga_projects, get_project_from_orga 
from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.elastic import reset_index_scanr, refresh_index
from project.server.main.scanr2 import get_publications_for_project, get_domains_from_publications
from project.server.main.export_data_without_tunnel import dump_from_http
from project.server.main.paysage import get_correspondance_paysage, get_main_id_paysage

import pysftp
import pickle
import requests
from bs4 import BeautifulSoup
import re
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

PARTICIPANTS_CODED = pickle.load(open('/src/project/server/main/participants_coded.pkl', 'rb'))
logger.debug(f'{len(PARTICIPANTS_CODED)} participants coded loaded')

def contient_lettre(s: str) -> bool:
    return any(c.isalpha() for c in s)

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
        dump_from_http('projects', 50)
    if args.get('reload_index_only', False) is False:
        if args.get('publi', True):
            save_to_mongo_publi_indexes()
        if args.get('v2', False):
            df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects-v2.jsonl.gz', lines=True)
        else:
            df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz', lines=True)
        phc_duplicates = get_phc_duplicates(df)
        projects = [p for p in df.to_dict(orient='records') if p['id'] not in phc_duplicates]
        participations = []
        orga_map = get_orga_map()
        os.system('rm -rf /upw_data/scanr/projects_denormalized.jsonl')
        os.system('rm -rf /upw_data/scanr/participations_denormalized.jsonl')
        projects = [p for p in projects if p.get('type') not in ['Casdar']]
        corresp_paysage = get_correspondance_paysage()
        for ix, p in enumerate(projects):
            # rename with priorities, domains will be used later down
            if (isinstance(p.get('domains'), list)) and not (isinstance(p.get('priorities'), list)):
                p['priorities'] = p['domains']
            # split keywords
            if isinstance(p['keywords'], dict):
                for lang in p['keywords']:
                    if isinstance(p['keywords'][lang], list):
                        new_keywords = []
                        for k in p['keywords'][lang]:
                            new_keywords += [w.strip() for w in re.split(r'[,;]', k)]
                        p['keywords'][lang] = new_keywords
            denormalized_affiliations = []
            participants = []
            if isinstance(p.get('participants'), list):
                participants = p.get('participants')
            for part in participants:
                is_identified=False
                participant_label = part.get('label', {})
                participant_name = 'participant'
                if isinstance(participant_label.get('default'), str):
                    participant_name = participant_label['default'].split('__-__')[0]
                part_id = part.get('structure')
                if part_id:
                    part['participant_id'] = part.pop('structure')
                if part_id is None and participant_name.lower() in PARTICIPANTS_CODED:
                    for coded_id in ['rnsr', 'paysage', 'siret', 'siren', 'ror', 'grid']:
                        if PARTICIPANTS_CODED[participant_name.lower()].get(coded_id):
                            part_id = PARTICIPANTS_CODED[participant_name.lower()].get(coded_id)
                            #denormalized_organization = get_orga(orga_map, part_id)
                            #if 'label' in denormalized_organization:
                            #    logger.debug(f'got {part_id} from hand coded table for {participant_name}')
                            #    break
                if part_id and (not part_id.startswith('pic')):
                    is_identified=True
                    part_id = get_main_id_paysage(part_id, corresp_paysage)
                    denormalized_organization = get_orga(orga_map, part_id)
                    part['structure'] = denormalized_organization
                    denormalized_affiliations.append(denormalized_organization)
                participant_key = f'{participant_name}---{is_identified}'
                if is_identified:
                    participant_to_identify = 'identified'
                else: 
                    participant_to_identify = participant_name
                part['participant_key'] = participant_key
                part['participant_to_identify'] = participant_to_identify
            co_countries = get_co_occurences(denormalized_affiliations, 'country')
            if co_countries:
                projects[ix]['co_countries'] = co_countries
            structures_to_combine = [a for a in denormalized_affiliations if ((isinstance(a.get('kind'), list)) and ('Structure de recherche' in a.get('kind', [])) and (a.get('status') == 'active'))]
            co_structures = get_co_occurences(structures_to_combine, 'id_name')
            if co_structures:
                projects[ix]['co_structures'] = co_structures
            institutions_to_combine = [a for a in denormalized_affiliations if ((isinstance(a.get('kind'), list)) and ('Structure de recherche' not in a.get('kind', [])) and (a.get('status') == 'active'))]
            co_institutions = get_co_occurences(institutions_to_combine, 'id_name')
            if co_institutions:
                projects[ix]['co_institutions'] = co_institutions
            text_to_autocomplete = []
            for lang in ['default', 'en', 'fr']:
                for k in ['label', 'acronym']:
                    if isinstance(p.get(k), dict):
                        if isinstance(p[k].get(lang), str):
                            text_to_autocomplete.append(p[k][lang])
            publications_data = {}
            publis_to_expose = []
            publicationsCount = 0
            if args.get('publi', True):
                publications_data = get_publications_for_project(p['id'])
                for pub in publications_data.get('publications', []):
                    simple_publi = {}
                    for f in ['id', 'projects', 'title', 'affiliations']:
                        if pub.get(f):
                            simple_publi[f] = pub[f]
                    if simple_publi:
                        publis_to_expose.append(simple_publi)
                publicationsCount = publications_data.get('count', 0)
                logger.debug(f"{publicationsCount} publications retrieved for project {p['id']}")
            projects[ix]['publications'] = publis_to_expose
            projects[ix]['publicationsCount'] = publicationsCount
            domains_infos = get_domains_from_publications(publications_data.get('publications', []))
            projects[ix].update(domains_infos)
            text_to_autocomplete.append(p['id'])
            text_to_autocomplete = list(set(text_to_autocomplete))
            projects[ix]['autocompleted'] = text_to_autocomplete
            projects[ix]['autocompletedText'] = text_to_autocomplete
            
            title_abs_text = ''
            for field in ['label', 'description', 'keywords']:
                if isinstance(projects[ix].get(field), dict):
                    for lang in ['fr', 'en']:
                        if isinstance(projects[ix][field].get(lang), str):
                            title_abs_text += projects[ix][field][lang]+' '
            projects[ix]['title_abs_text'] = title_abs_text
            formatted_participations = get_participations(projects[ix], orga_map)
            if formatted_participations:
                participations += formatted_participations 
        to_jsonl(projects, '/upw_data/scanr/projects_denormalized.jsonl') 
        to_jsonl(participations, '/upw_data/scanr/participations_denormalized.jsonl') 
    os.system(f'cd {MOUNTED_VOLUME}scanr && rm -rf projects_denormalized.jsonl.gz && gzip -k projects_denormalized.jsonl')
    upload_object(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/projects_denormalized.jsonl.gz', destination='production/projects_denormalized.jsonl.gz')
    chunk_project = 500
    if args.get('publi', True):
        chunk_project = 50
    load_scanr_projects('/upw_data/scanr/projects_denormalized.jsonl', index_name, chunk_project) 
    os.system(f'cd {MOUNTED_VOLUME}scanr && rm -rf participations_denormalized.jsonl.gz && gzip -k participations_denormalized.jsonl')
    upload_object(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/participations_denormalized.jsonl.gz', destination='production/participations_denormalized.jsonl.gz')
    load_scanr_projects('/upw_data/scanr/participations_denormalized.jsonl', index_name.replace('project', 'participation'), 500) 

def test(project_id):
    orga_map = get_orga_map()
    df = pd.read_json('/upw_data/scanr/projects_denormalized.jsonl', lines=True)
    for p in df.to_dict(orient='records'):
        if p['id'] == project_id:
            break
    return get_participations(p, orga_map)

def get_participations(project, orga_map):
    participations = []
    if isinstance(project.get('participants', []), list):
        for p in project['participants']:
            if isinstance(p.get('structure'), dict):
                new_part = {}
                # for e in ['id', 'kind', 'label', 'acronym', 'status', 'institutions', 'parents']
                for f in ['id', 'id_name', 'id_name_default', 'kind', 'country', 'label', 'acronym', 'status', 'isFrench', 'role', 'funding']:
                    if f in p['structure']:
                        new_part[f'participant_{f}'] = p['structure'][f]
                if new_part and new_part.get('participant_id'):
                    if new_part['participant_id'] not in [k['participant_id'] for k in participations]:
                        participations.append(new_part)
                if isinstance(p['structure'].get('institutions'), list):
                    for inst in p['structure'].get('institutions'):
                        if inst.get('relationType') in ['établissement tutelle'] and inst.get('structure'):
                            new_part = {}
                            current_part = get_orga(orga_map, inst['structure'])
                            for f in ['id', 'id_name', 'id_name_default', 'kind', 'country', 'label', 'acronym', 'status', 'isFrench']:
                                if f in current_part:
                                    new_part[f'participant_{f}'] = current_part[f]
                            if new_part and new_part.get('participant_id'):
                                if new_part['participant_id'] not in [k['participant_id'] for k in participations]:
                                    participations.append(new_part)
    part_ids = [k['participant_id'] for k in participations]
    assert(len(part_ids) == len(set(part_ids)))
    for part in participations:
        for f in ['id', 'type', 'year', 'budgetTotal', 'budgetFinanced']:
            if f in project:
                part[f'project_{f}']=project[f]
        if ('project_budgetTotal' not in part) or (part.get('project_budgetTotal') != part.get('project_budgetTotal')):
            if ('project_budgetFinanced' in part) and (part.get('project_budgetFinanced')==part.get('project_budgetFinanced')):
                part['project_budgetTotal'] = part['project_budgetFinanced']
        for f in ['partiticpant_institutions']:
            if f in part:
                del part[f]
        part['participant_type'] = 'other'
        if 'participant_kind' in part:
            if isinstance(part.get('participant_kind'), list) and ('Structure de recherche' in part['participant_kind']) and isinstance(part.get('participant_id'), str) and (contient_lettre(part['participant_id'])):
                part['participant_type'] = 'laboratory'
            else:
                part['participant_type'] = 'institution'
        if part.get('participant_isFrench'):
            pass
        else:
            part['participant_isFrench'] = False
        current_part = get_orga(orga_map, part['participant_id'])
        if current_part and isinstance(current_part.get('mainAddress'), dict):
            address = current_part.get('mainAddress')
            new_address = {}
            if isinstance(address.get('gps'), dict):
                new_address['gps'] = address['gps']
                if address['gps'].get('lat') and address['gps'].get('lon'):
                    if 'participant_id_name_default' not in part:
                        logger.debug('NO ID NAME ??? for ')
                        logger.debug(part)
                    new_address['gps_id_name'] = str(address['gps']['lat'])+'_'+str(address['gps']['lon'])+'_'+part['participant_id']+'_'+part.get('participant_id_name_default', 'noname')
            for f in ['address', 'postcode', 'city', 'country', 'region']:
                if isinstance(address.get(f), str):
                    new_address[f] = address[f]
            if new_address:
                part['address'] = new_address
    for part in participations:
        part['co_partners_fr_labs'] = list(set([k['participant_id_name'] for k in participations if (k['participant_id'] != part['participant_id']) and (k['participant_type'] == 'laboratory') and (k.get('participant_isFrench')) and k.get('participant_id_name')]))
        part['co_partners_fr_inst'] = list(set([k['participant_id_name'] for k in participations if (k['participant_id'] != part['participant_id']) and (k['participant_type'] != 'laboratory') and (k.get('participant_isFrench')) and k.get('participant_id_name')]))
        part['co_partners_foreign_inst'] = list(set([k['participant_id_name'] for k in participations if (k['participant_id'] != part['participant_id']) and (k.get('participant_isFrench') == False) and k.get('participant_id_name')]))
        try:
            part['participant_isFrench']
        except:
            logger.debug(part)
    return participations

def load_scanr_projects(scanr_output_file_denormalized, index_name, chunksize=50):
    denormalized_file=scanr_output_file_denormalized
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-projects index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={denormalized_file} --output={es_host}{index_name} --type=data --limit {chunksize} --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)
    refresh_index(index_name)
