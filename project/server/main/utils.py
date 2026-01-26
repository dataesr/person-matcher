import json
import pandas as pd
import itertools
from project.server.main.logger import get_logger
import pymongo
import re
import os

NB_MAX_CO_ELEMENTS = 20

logger = get_logger(__name__)
    
EXCLUDED_ID = ['881000251']

MOUNTED_VOLUME = '/upw_data/'

def clean_discipline(x):
    if x in ['Archaeology', 'Psychology', 'Psychological Sciences']:
        return 'Humanities'
    if x in ['Astronomy and Physics']:
        return 'Physics and Astronomy']
    return x

def to_mongo_cache(input_list, collection_name):
    logger.debug(f'importing {len(input_list)} {collection_name}')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}{collection_name}_cache.jsonl'
    pd.DataFrame(input_list).to_json(output_json, lines=True, orient='records')
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    os.system(mongoimport)
    mycol = mydb[collection_name]
    for f in ['id']:
        mycol.create_index(f)
    os.remove(output_json)
    myclient.close()

def get_main_id(current_id, correspondance):
    if current_id in correspondance:
        for c in correspondance[current_id]:
            if c.get('main_id'):
                return c['main_id']
    return current_id

def identifier_type(identifiant: str):
    """
    Détermine le type d'identifiant parmi :
    siren, siret, grid, ror, rnsr

    Retourne le nom du type ou None si aucun ne correspond.
    """

    if not identifiant:
        return None

    identifiant = identifiant.strip()

    # SIREN : 9 chiffres
    if re.fullmatch(r"\d{9}", identifiant):
        return "siren"

    # SIRET : 14 chiffres
    if re.fullmatch(r"\d{14}", identifiant):
        return "siret"

    # GRID : commence par 'grid.'
    if identifiant.startswith("grid."):
        return "grid"

    # ROR : regex fournie
    if re.fullmatch(r"^0[a-hj-km-np-tv-z0-9]{6}[0-9]{2}$", identifiant):
        return "ror"

    # RNSR : 10 caractères alphanumériques
    if re.fullmatch(r"[A-Za-z0-9]{10}", identifiant):
        return "rnsr"

    if identifiant.startswith('ED'):
        return 'ed'

    if len(identifiant)==5:
        return 'paysage'
    
    if len(identifiant)==8:
        return 'uai'

    return None


def save_to_mongo_publi_indexes():
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publi_meta'
    logger.debug('indices on publi_meta')
    mycol = mydb[collection_name]
    mycol.create_index('id')
    mycol.create_index('authors.person')
    mycol.create_index('affiliations')
    mycol.create_index('projects')
    myclient.close()

def get_co_occurences(my_list, my_field):
    elts_to_combine = [a for a in my_list if a.get(my_field)]
    values_to_combine = list(set([a[my_field] for a in elts_to_combine]))
    values_to_combine.sort()
    if len(values_to_combine) <= NB_MAX_CO_ELEMENTS:
        combinations = list(set(itertools.combinations(values_to_combine, 2)))
        combinations.sort()
        res = [f'{a}---{b}' for (a,b) in combinations]
        return res
    return None

def remove_duplicates(x, main_id):
    natural_id_fields = ['structure', 'relationType', 'fromDate', 'person', 'role', 'firstName', 'lastName']
    res = []
    already_there = set()
    for e in x:
        natural_id_elts = []
        for f in natural_id_fields:
            natural_id_elts.append(e.get(f, ''))
        natural_id = ';;;'.join(natural_id_elts)
        e['natural_id'] = natural_id
        if natural_id not in already_there:
            res.append(e)
            already_there.add(natural_id)
        else:
            logger.debug(f"removing duplicate in {main_id} : {natural_id}")
    return res 
       
def build_ed_map():
    df_ed = pd.read_csv('./ed_idref.tsv', sep='\t')
    df_ed.columns = ['ed', 'idref', 'label', 'paysage']
    df_ed['ed'] = df_ed['ed'].apply(lambda x:'ED'+str(x))
    ed_map = {}
    for r in df_ed.itertuples():
        ed_map[r.ed]={'id': r.ed, 'endDate': None, 'status': 'active', 
                      'label':{'fr': r.label, 'default': r.label},
                      'kind': ['Secteur public'],
                      'level': 'École doctorale',
                     'legalCategory': {},
                      'nature': 'Ecole doctorale',
                      'isFrench': True,
                      'externalIds': [{'id': r.ed, 'type': 'ed'},
                                      {'id': 'idref'+r.idref, 'type': 'idref'}]}
    return ed_map

def orga_with_ed_deprecated():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    df = df[~df.id.isin(EXCLUDED_ID)]
    orga = df.to_dict(orient='records')
    ed_map = build_ed_map()
    updated = set()
    for org in orga:
        for f in ['institutions', 'parents', 'leaders']:
            if org.get(f):
                org[f] = remove_duplicates(org[f], org['id'])
        if org['id'] in ed_map:
            org.update(ed_map[org['id']])
            updated.add(org['id'])
    for org in ed_map.values():
        if org['id'] not in updated:
            #print(org['id'])
            orga.append(org)
    return orga

def get_all_manual_matches():
    old_matches = pd.read_csv('manual_matches.csv.gz')
    new_matches = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vRtJvpjh4ySiniYVzgUYpGQVQEuNY7ZOpqPbi3tcyRfKiBaLnAgYziQgecX_kvwnem3fr0M34hyCTFU/pub?gid=1281340758&single=true&output=csv')
    matches = pd.concat([old_matches, new_matches])
    return matches

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def clean_json(elt):
    if isinstance(elt, dict):
        keys = list(elt.keys()).copy()
        for f in keys:
            if (not elt[f] == elt[f]) or (elt[f] is None):
                del elt[f]
            else:
                elt[f] = clean_json(elt[f])
    elif isinstance(elt, list):
        for ix, k in enumerate(elt):
            elt[ix] = clean_json(elt[ix])
    return elt

def clean_json_old(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')

def to_json(input_list, output_file, ix):
    if ix == 0:
        mode = 'w'
    else:
        mode = 'a'
    with open(output_file, mode) as outfile:
        if ix == 0:
            outfile.write('[')
        for jx, entry in enumerate(input_list):
            if ix + jx != 0:
                outfile.write(',\n')
            json.dump(entry, outfile)
