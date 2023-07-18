from project.server.main.association_matcher import association_match
from project.server.main.idref_matcher import name_idref_match
from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object
from project.server.main.utils import chunks, to_jsonl

import requests
import multiprocess as mp
import os
import json
import pymongo
import pandas as pd

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'
SUDOC_SERVICE = os.getenv('SUDOC_SERVICE')

def get_manual_match():
    download_object('misc', 'manual_idref.json', '/upw_data/manual_idref.json')
    download_object('misc', 'orcid_idref.jsonl', '/upw_data/orcid_idref.jsonl')
    publi_author_dict = {}
    manual_infos = pd.read_json('/upw_data/manual_idref.json', lines=True).to_dict(orient='records')
    orcid_infos = pd.read_json('/upw_data/orcid_idref.jsonl', lines=True).to_dict(orient='records')
    infos = manual_infos + orcid_infos
    for a in infos:
        author_key = None
        if normalize(a.get('first_name'), remove_space=True) and normalize(a.get('last_name'), remove_space=True):
            author_key = normalize(a.get('first_name'), remove_space=True)[0]+normalize(a.get('last_name'), remove_space=True)
        elif normalize(a.get('full_name'), remove_space=True):
            author_key = normalize(a.get('full_name'), remove_space=True)
        publi_id = a.get('publi_id')
        person_id = a.get('person_id')
        if author_key and publi_id and person_id:
            publi_author_dict[f'{publi_id.strip()};{author_key}'] = person_id.strip()
    return publi_author_dict


def get_publications_from_author_key(author_key):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb['person_matcher_input']
    res = list(mycoll.find({'authors.author_key': author_key}))
    myclient.close()
    return res


def match_all(author_keys):

    for author_key in author_keys:
        match(author_key)

def pre_process_publications(args):
    index_name = args.get('index')

    # files are splitted in /upw_data/...

        # TODO : put in person-matcher
        #os.system(f'rm -rf {enriched_output_file}')
        #for f in is os.listdir('/upw_data/'):
        #    if f.startswith(f'{split_prefix})' and '_extract' not in f:
        #        os.system(f'cat /upw_data/{f} >> {enriched_output_file}')
    
    logger.debug('dropping collection person_matcher_input')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb['person_matcher_input']
    mycoll.drop()
    
    logger.debug('dropping collection person_matcher_output')
    mydb['person_matcher_output'].drop()
    myclient.close()

    manuel_matches = get_manual_match()

    df_all = pd.read_json(f'{MOUNTED_VOLUME}/{index_name}.jsonl', lines=True, chunksize=25000)
    author_keys = []
    ix = 0
    for df in df_all:
        logger.debug(f'reading {ix} chunks of publications')
        publications = df.to_dict(orient='records')
        prepared = prepare_publications(publications, manuel_matches)
        save_to_mongo_preprocessed(prepared['relevant'])
        author_keys += prepared['author_keys']
        author_keys = list(set(author_keys))
        ix += 1
    logger.debug(f'{len(author_keys)} author_keys detected')
    json.dump(author_keys, open(f'{MOUNTED_VOLUME}/author_keys.json', 'w'))


def get_publis_meta(publications):
    metas = []
    for p in publications:
        if 'id' not in p:
            continue
        elt = {'id': p['id']}
        if 'year' in p:
            elt['year'] = p['year']
        # domains
        domains = []
        if isinstance(p.get('classifications'), list):
            for c in p['classifications']:
                if c.get('label'):
                    domain = {'label': {'default': c['label']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = c.get('reference')
                    domains.append(domain)
                if c.get('label_fr'):
                    domain = {'label': {'default': c['label_fr']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = c.get('reference')
                    domains.append(domain)
        if isinstance(p.get('hal_classifications'), list):
            for c in p['hal_classifications']:
                if c.get('label'):
                    domain = {'label': {'default': c['label']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = 'HAL'
                    domains.append(domain)
        if isinstance(p.get('thematics'), list):
            for c in p['thematics']:
                if c.get('fr_label'):
                    domain = {'label': {'default': c.get('fr_label')}}
                    domain['code'] = c.get('code')
                    domain['type'] = c.get('reference')
        if p.get('bso_classification') and isinstance(p.get('bso_classification'), str):
            domains.append({'label': {'default': p['bso_classification']}, 'type': 'bso_classification'})
        if isinstance(p.get('bsso_classification'), dict) and isinstance(p['bsso_classification'].get('field'), str):
            domains.append({'label': {'default': p['bsso_classification']['field']}, 'type': 'bsso_classification'})
        #if p.get('sdg_classification'):
        #    domains.append({'label': {'default': p['bso_classification']}, 'type': 'bso_classification'})
        # keywords
        keywords = []
        if isinstance(p.get('keywords'), list):
            for k in p['keywords']:
                if k.get('keyword'):
                    keywords.append(k['keyword'])
                    domains.append({'label': {'default': k['keyword']}, 'type': 'keyword'})
        if keywords:
            elt['keywords'] = {'default': keywords}
        if domains:
            elt['domains'] = domains
        if elt:
            metas.append(elt)
    return metas
        

def prepare_publications(publications, manuel_matches):
    relevant_infos = []
    author_keys = []
    # keeping and enriching only with relevant info for person matching
    for p in publications:
        new_elt = {}
        if 'datasource' in p:
            new_elt['datasource'] = p['datasource']
        else:
            logger.debug(f'missing datasource !! in {p}')

        #for f in ['id', 'doi', 'nnt_id', 'title_first_author']:
        for f in ['id']:
            if f in p:
                new_elt['id'] = p[f]
                break

        if new_elt.get('id') is None:
            logger.debug(f'no id for {p}')
            continue

        authors = p.get('authors', [])
        if not isinstance(authors, list):
            authors = []

        if not authors:
            # no authors to identify
            continue

        new_elt['nb_authors'] = len(authors)
        new_elt['authors'] = []

        entity_linked = []
        for a in authors:
            author_key = None
            current_author = {}
            if normalize(a.get('first_name'), remove_space=True) and normalize(a.get('last_name'), remove_space=True):
                author_key = normalize(a.get('first_name'), remove_space=True)[0]+normalize(a.get('last_name'), remove_space=True)
                current_author['last_name'] = a['last_name']
                current_author['first_name'] = a['first_name']
            elif normalize(a.get('full_name'), remove_space=True):
                author_key = normalize(a.get('full_name'), remove_space=True)
                current_author['full_name'] = a['full_name']

            manual_check_key = f"{p.get('id')};{author_key}"
            if manual_check_key in manuel_matches:
                a['idref'] = manuel_matches[manual_check_key].replace('idref', '')
                logger.debug(f"setting {a['idref']} for {manual_check_key} from manual input!")

            if author_key and len(author_key) > 4:
                current_author['author_key'] = author_key
                entity_linked.append(author_key)
                author_keys.append(author_key)

            if a.get('id') and isinstance(a['id'], str) and 'idref' in a.get('id'):
                a['idref'] = a['id'].replace('idref', '')
            
            if 'role' in a:
                current_author['role'] = a['role']
            for f in ['idref', 'id_hal_s', 'orcid']:
                if a.get(f):
                    current_id = f+str(a[f])
                    if f == 'orcid':
                        current_id = normalize(current_id, remove_space=True)
                    entity_linked.append(current_id)
                if a.get('idref'):
                    current_author['id'] = f"idref{a['idref']}"
                if a.get('affiliations') and isinstance(a['affiliations'], list):
                    aff = []
                    for aff in a['affiliations']:
                        if isinstance(aff.get('ids'), list):
                            aff = [k['id'] for k in aff['ids'] if 'id' in k]
                    if aff:
                        current_author['affiliations'] = aff
            new_elt['authors'].append(current_author)

        
        issns = p.get('journal_issns', '')
        if isinstance(issns, str) and issns:
            for elt in issns.split(','):
                entity_linked.append('issn'+normalize(elt, remove_space=True))
        
        for other_entity in ['keywords']:
            elts = p.get(other_entity, [])
            if not isinstance(elts, list):
                continue
            for elt in elts:
                entity_linked.append(normalize(elt, remove_space=True))
        
        affiliations = p.get('affiliations')
        if isinstance(affiliations, list) and affiliations:
            for aff in affiliations:
                for identifier in aff.get('ids', []):
                    if identifier.get('type') == 'rnsr':
                        entity_linked.append(normalize(identifier['id'], remove_space=True))
        
        classifications = p.get('classifications')
        if isinstance(classifications, list) and classifications:
            for classification in classifications:
                if classification.get('reference') == 'wikidata':
                    entity_linked.append('wikidata'+classification.get('code'))

        entity_linked = [e for e in list(set(entity_linked)) if e]
        new_elt['entity_linked'] = entity_linked
        relevant_infos.append(new_elt)
    return {'relevant': relevant_infos, 'author_keys': author_keys}

def save_to_mongo_preprocessed(relevant_infos):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}person-matcher-current.jsonl'
    pd.DataFrame(relevant_infos).to_json(output_json, lines=True, orient='records')
    collection_name = 'person_matcher_input'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('authors.author_key')
    logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
    myclient.close()

def save_to_mongo_results(results, author_key):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}{author_key[0:100]}.jsonl'
    pd.DataFrame(results).to_json(output_json, lines=True, orient='records')
    collection_name = 'person_matcher_output'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('publication_id')
    mycol.create_index('author_key')
    mycol.create_index('person_id')
    logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
    myclient.close()

def match(author_key, idx=None):

    force_download = True

    if idx:
        logger.debug(f'match author_key number {idx}')
        
    publications = get_publications_from_author_key(author_key)

    #if not are_publications_prepared:
    #    publications = prepare_publications(publications)

    # 1. on commence par mettre les ids déjà connus
    for p in publications:
        authors = p.get('authors')
        if not isinstance(authors, list):
            continue
        for aut in authors:
            if aut.get('author_key') == author_key and isinstance(aut.get('id'), str):
                p['person_id'] = {'id': aut['id'], 'method': 'input'}

    # 2. matching par associations d'éléments communs : coauteurs, keywords, issn ...
    associated = association_match(publications, author_key)
    publications = associated['publications']

    # 3. on complète par des match idref direct sur nom / prenom
    missing_ids = 0
    elements_to_match = {}
    for p in publications:
        if p.get('person_id') is None:
            missing_ids += 1
            authors = p.get('authors')
            if not isinstance(authors, list):
                continue
            for a in p.get('authors', []):
                if a.get('author_key') == author_key:
                    elt_key = f"{normalize(a.get('first_name'))};;;{normalize(a.get('last_name'))};;;{normalize(a.get('full_name'))}"
                    elt = {'first_name': a.get('first_name'), 'last_name': a.get('last_name'), 'full_name': a.get('full_name')}
                    elements_to_match[elt_key] = elt

    logger.debug(f'{missing_ids} missing ids / {len(publications)} publications for {author_key}')
    
    for elt_key in elements_to_match:
        first_name = elements_to_match[elt_key]['first_name']
        last_name = elements_to_match[elt_key]['last_name']
        full_name = elements_to_match[elt_key]['full_name']
        idref = name_idref_match(first_name, last_name, full_name)
        elements_to_match[elt_key]['idref'] = idref

    
    for p in publications:
        if p.get('person_id') is None:
            for a in p.get('authors', []):
                if a.get('author_key') == author_key:
                    elt_key = f"{normalize(a.get('first_name'))};;;{normalize(a.get('last_name'))};;;{normalize(a.get('full_name'))}"
                    if elements_to_match[elt_key]['idref']:
                        p['person_id'] = elements_to_match[elt_key]['idref']

    # TODO : n'ajouter que les match qui ne sont pas déjà en base !
    results = []
    idrefs = []
    for p in publications:
        if p.get('person_id'):
            idrefs.append(p['person_id']['id'].replace('idref',''))
            results.append({
                'publication_id': p['id'],
                'year': p.get('year'),
                'role': p.get('role', 'author'),
                'affiliations': p.get('affiliations', []),
                'author_key': author_key,
                'person_id': p['person_id']['id'],
                'person_id_match_method': p['person_id']['method']
                })
    save_to_mongo_results(results, author_key)
    idrefs = list(set(idrefs))
    if idrefs:
        requests.post(f'{SUDOC_SERVICE}/harvest', json={'idrefs': idrefs, 'force_download': force_download}) 
    return results
        
