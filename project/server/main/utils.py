import json
import pandas as pd
import itertools
from project.server.main.logger import get_logger
import pymongo

NB_MAX_CO_ELEMENTS = 20

logger = get_logger(__name__)
    
EXCLUDED_ID = ['881000251']

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
        
def orga_with_ed():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    df = df[~df.id.isin(EXCLUDED_ID)]
    orga = df.to_dict(orient='records')
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
