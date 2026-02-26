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
from project.server.main.utils_rnsr import parse_address 
from project.server.main.paysage import get_paysage_data, get_main_id_paysage, get_correspondance_paysage
from project.server.main.s3 import upload_object
from project.server.main.export_data_without_tunnel import get_with_retry
from project.server.main.paysage import get_correspondance_paysage
from project.server.main.utils import identifier_type
from project.server.main.logger import get_logger

logger = get_logger(__name__)

DATAESR_URL = os.getenv('DATAESR_URL')
BASE_URL_RNSR = f'{DATAESR_URL}/fetchers/rnsr'

rnsr_key_struct_dict = json.load(open('rnsr_key.json', 'r'))
rnsr_key_leaders_dict = json.load(open('rnsr_key_leaders.json', 'r'))

def get_leaders():
    r = get_with_retry(f'{BASE_URL_RNSR}/leaders')
    leaders = r.json()['data']
    leaders_dict = {}
    for k in leaders:
        leaders_dict[k['rnsr_key']] = k
        if k['rnsr_key'] in rnsr_key_leaders_dict:
           leaders_dict[k['rnsr_key']]['id'] = rnsr_key_leaders_dict[k['rnsr_key']]
    logger.debug(f"got {len(leaders_dict)} leaders key from rnsr")
    return leaders_dict
# sites r = requests.get(f'{BASE_URL_RNSR}/sites', headers = headers_185)

def get_all_rnsr_ids():
    r = get_with_retry(f'{BASE_URL_RNSR}/structures/updates')
    all_rnsr = r.json()['data']
    return all_rnsr

def get_one_rnsr(rnsr):
    r = get_with_retry(f'{BASE_URL_RNSR}/structures/{rnsr}')
    return r.json()['data']

def get_all_rnsr_data():
    data = []
    all_rnsr = get_all_rnsr_ids()
    for ix, e in enumerate(all_rnsr):
        data.append(get_one_rnsr(e))
        if ix % 500 == 0 and ix > 0:
            logger.debug(f"getting all rnsr data, {ix}")
    pd.DataFrame(data).to_json('/upw_data/scanr/orga_ref/rnsr_extract.jsonl', lines=True, orient='records')

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

def transform(elt, leaders_dict):
    today = str(datetime.today())[0:10]
    new = {}
    new['id'] = elt['id']
    if 'level' in elt:
        new['level'] = elt['level']
    # dates
    if isinstance(elt.get('dates'), dict):
        if isinstance(elt['dates'].get('start_date'), str):
            new['startDate'] =elt['dates']['start_date']
            assert(len(new['startDate'])==19)
            new['creationYear'] = new['startDate'][0:4]
        if isinstance(elt['dates'].get('end_date'), str):
            new['endDate'] =elt['dates']['end_date']
            assert(len(new['endDate'])==19)
            if new['endDate'] > today:
                new['status'] = 'active'
            else:
                new['status'] = 'old'
        else:
            new['status'] = 'active'
    if isinstance(elt.get('website'), str):
        new['links'] = [{'type': 'main', 'url': elt['website']}]
    alias = []
    if isinstance(elt.get('name'), dict):
        if isinstance(elt['name'].get('label'), str):
            new['label'] = {'fr': elt['name']['label'], 'default': elt['name']['label']}
            alias.append(elt['name']['label'])
        if isinstance(elt['name'].get('acronym'), str):
            new['acronym'] = {'fr': elt['name']['acronym'], 'default': elt['name']['acronym']}
            alias.append(elt['name']['acronym'])
    new['alias'] = alias
    if isinstance(elt.get('type'), str):
        new['kind'] = [elt['type']]
    new['nature'] = 'Unite propre'
    new['externalIds'] =  [{'id': new['id'], 'type': 'rnsr'}]
    if isinstance(elt.get('code_numbers'), list):
        for c in elt['code_numbers']:
            new['externalIds'].append({'id': c, 'type': 'label_numero'})
    new['isFrench'] = True
    if isinstance(elt.get('input_address'), str):
        new['address'] = [parse_address(elt['input_address'])]
    new_leaders, new_leaders_active = [], []
    if isinstance(elt.get('leaders'), list):
        for c in elt['leaders']:
            new_lead = {}
            active_lead = True
            if isinstance(c.get('start_date'), str):
                new_lead['startDate'] = c['start_date']
            if isinstance(c.get('end_date'), str):
                new_lead['endDate'] = c['end_date']
                if new_lead['endDate'] < today:
                    active_lead = False
            if isinstance(c.get('role'), str):
                new_lead['role'] = c['role']
            new_lead['rnsr_key'] = c.get('rnsr_key')
            if c['rnsr_key'] in leaders_dict:
                if isinstance(leaders_dict[c['rnsr_key']].get('first_name'), str):
                    new_lead['firstName'] = leaders_dict[c['rnsr_key']].get('first_name').title()
                if isinstance(leaders_dict[c['rnsr_key']].get('last_name'), str):
                    new_lead['lastName'] = leaders_dict[c['rnsr_key']].get('last_name').title()
                if leaders_dict[c['rnsr_key']].get('id'):
                    new_lead['person'] = leaders_dict[c['rnsr_key']].get('id') 
                new_leaders.append(new_lead)
                if active_lead:
                    new_leaders_active.append(new_lead)
    new['leaders'] = new_leaders_active
    new['leaders_all'] = new_leaders
    
    new_institutions = []
    if isinstance(elt.get('supervisors'), list):
        for s in elt['supervisors']:
            new_inst = {}
            if isinstance(s.get('start_date'), str):
                new_inst['startDate'] = s['start_date']
            if isinstance(s.get('end_date'), str):
                new_inst['endDate'] = s['end_date']
            if isinstance(s.get('supervision_type'), str):
                new_inst['relationType'] = s.get('supervision_type')
            new_inst['rnsr_key'] = s.get('rnsr_key')
            if s['rnsr_key'] not in rnsr_key_struct_dict:
                logger.debug(f"{s.get('rnsr_key')} not in rnsr key dict")
                logger.debug(s)
                logger.debug(elt)
                logger.debug('il faut compléter le fichier rnsr_key.jsonl')
            new_inst['structure'] = rnsr_key_struct_dict[s['rnsr_key']]
            if isinstance(s.get('name'), str):
                new_inst['label'] = s['name']
                new_institutions.append(new_inst)
    new['institutions'] = new_institutions
    
    new_parents = []
    if isinstance(elt.get('parents'), list):
        for p in elt['parents']:
            new_p = {}
            if isinstance(p.get('start_date'), str):
                new_p['startDate'] = p['start_date']
            if isinstance(p.get('end_date'), str):
                new_p['endDate'] = p['end_date']
            if isinstance(p.get('id'), str):
                new_p['structure'] = p['id']
            if p.get('exclusive') is True:
                new_p['relationType'] = 'Exclusif'
            else:
                new_p['relationType'] = 'Non exclusif'
            new_parents.append(new_p)
    new['parents'] = new_parents
    
    if isinstance(elt.get('description'), str) and len(elt.get('description').split(' '))>10:
        new['description'] = {'fr': elt['description'], 'default': elt['description']}
        
    new_relations = []
    if isinstance(elt.get('doctoral_schools'), list):
        for r in elt['doctoral_schools']:
            new_r = {}
            if isinstance(r.get('start_date'), str):
                new_r['startDate'] = r['start_date']
            if isinstance(r.get('end_date'), str):
                new_r['endDate'] = r['end_date']
            if isinstance(r.get('ed'), str):
                new_r['structure'] = r['ed'].upper()
                new_r['type'] = 'DS_LABS'
                new_relations.append(new_r)
    new['relations'] = new_relations
    
    new_pred = []
    if isinstance(elt.get('predecessors'), list):
        for p in elt['predecessors']:
            new_p = {}
            if isinstance(p.get('succession_date'), str):
                new_p['eventDate'] = p['succession_date']
            if isinstance(p.get('succession_type'), str):
                new_p['eventType'] = p['succession_type']
            if isinstance(p.get('id'), str):
                new_p['structure'] = p['id']
                new_pred.append(new_p)
    new['predecessors'] = new_pred
    
    for f in ['rnsr_domains', 'rnsr_themes', 'panels', 'sites']:
        if elt.get(f):
            new[f] = elt[f]
            
    return new

def dump_rnsr_data_v2(args):
    if args.get('update_rnsr_extract', False):
        get_all_rnsr_data()
    leaders_dict = get_leaders()
    df_rnsr = pd.read_json('/upw_data/scanr/orga_ref/rnsr_extract.jsonl', lines=True)
    data = []
    for k in df_rnsr.to_dict(orient='records'):
        n = transform(k, leaders_dict)
        data.append(n)
    os.system(f'rm -rf /upw_data/scanr/orga_ref/rnsr-v2.jsonl')
    to_jsonl(data, f'/upw_data/scanr/orga_ref/rnsr-v2.jsonl')

def format_rnsr():
    logger.debug('formatting RNSR data')
    df = pd.read_json(f'/upw_data/scanr/orga_ref/rnsr-v2.jsonl', lines=True)
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
        #e['categories'] = [{'id': 'z367d', 'name': 'Structure de recherche'}]
        e['categories'] = ['Structure de recherche']
        data.append(e)
    os.system('cd /upw_data/scanr/orga_ref/ && rm -rf rnsr_formatted.jsonl')
    to_jsonl(data, f'/upw_data/scanr/orga_ref/rnsr_formatted.jsonl')
    return data
        
