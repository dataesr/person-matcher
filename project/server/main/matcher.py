from project.server.main.association_matcher import association_match
from project.server.main.idref_matcher import name_idref_match
from project.server.main.strings import normalize
from project.server.main.logger import get_logger

import os
import json
import pymongo
import pandas as pd

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

def get_publications_from_author_key(author_key):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb['person_matcher_input']
    return list(mycoll.find({'authors.author_key': author_key}))


def pre_process_publications(args):
    logger.debug('dropping collection person_matcher_input')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb['person_matcher_input']
    mycoll.drop()

    df_all = pd.read_json(f'{MOUNTED_VOLUME}/test-scanr_full.jsonl', lines=True, chunksize=25000)
    author_keys = []
    ix = 0
    for df in df_all:
        logger.debug(f'reading {ix} chunks of publications')
        publications = df.to_dict(orient='records')
        prepared = prepare_publications(publications)
        save_to_mongo(prepared['relevant'])
        author_keys += prepared['author_keys']
        author_keys = list(set(author_keys))
        ix += 1
    logger.debug(f'{len(author_keys)} author_keys detected')
    json.dump(author_keys, open(f'{MOUNTED_VOLUME}/author_keys.json', 'w'))
    #return author_keys

def prepare_publications(publications):
    relevant_infos = []
    author_keys = []
    # keeping and enriching only with relevant info for person matching
    for p in publications:
        new_elt = {}
        if 'datasource' in p:
            new_elt['datasource'] = p['datasource']
        else:
            logger.debug(f'missing datasource !! in {p}')

        for f in ['id', 'doi', 'nnt_id', 'title_first_author']:
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
            logger.debug(f"no authors for {new_elt['id']}")
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

            if author_key and len(author_key) > 4:
                current_author['author_key'] = author_key
                entity_linked.append(author_key)
                author_keys.append(author_key)

            for f in ['idref', 'id_hal_s', 'orcid']:
                if a.get(f):
                    current_id = f+str(a[f])
                    if f == 'orcid':
                        current_id = normalize(current_id, remove_space=True)
                    entity_linked.append(current_id)
                if a.get('idref'):
                    current_author['id'] = f"idref{a['idref']}"
            new_elt['authors'].append(current_author)

        
        issns = p.get('journal_issns', '')
        if isinstance(issns, str) and issns:
            for elt in issns.split(','):
                entity_linked.append(normalize(elt, remove_space=True))
        
        for other_entity in ['keywords']:
            elts = p.get(other_entity, [])
            if not isinstance(elts, list):
                continue
            for elt in elts:
                entity_linked.append(normalize(elt, remove_space=True))

        entity_linked = [e for e in list(set(entity_linked)) if e]
        new_elt['entity_linked'] = entity_linked
        relevant_infos.append(new_elt)
    return {'relevant': relevant_infos, 'author_keys': author_keys}

def save_to_mongo(relevant_infos):
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

def match(publications, author_key):

    are_publications_prepared = False
    for p in publications:
        if p.get('nb_authors'):
            are_publications_prepared = True
            break
    if not are_publications_prepared:
        publications = prepare_publications(publications)

    publications = association_match(publications, author_key)
    missing_ids = 0
    elements_to_match = {}
    for p in publications:
        if p.get('cluster') is None or 'internal' in p.get('cluster'):
            missing_ids += 1
            for a in p.get('authors', []):
                if a.get('author_key') == author_key:
                    elt_key = f"{normalize(a.get('first_name'))};;;{normalize(a.get('last_name'))};;;{normalize(a.get('full_name'))}"
                    elt = {'first_name': a.get('first_name'), 'last_name': a.get('last_name'), 'full_name': a.get('full_name')}
                    elements_to_match[elt_key] = elt

    logger.debug(f'{missing_ids} missing ids / {len(publications)} publications for {author_key}')
    
    logger.debug(f'{elements_to_match}')
    
    for elt_key in elements_to_match:
        first_name = elements_to_match[elt_key]['first_name']
        last_name = elements_to_match[elt_key]['last_name']
        full_name = elements_to_match[elt_key]['full_name']
        idref = name_idref_match(first_name, last_name, full_name)
        elements_to_match[elt_key]['idref'] = idref

    logger.debug(f'{elements_to_match}')
    
    for p in publications:
        if p.get('cluster') is None or 'internal' in p.get('cluster'):
            for a in p.get('authors', []):
                if a.get('author_key') == author_key:
                    elt_key = f"{normalize(a.get('first_name'))};;;{normalize(a.get('last_name'))};;;{normalize(a.get('full_name'))}"
                    logger.debug(elt_key)
                    if elements_to_match[elt_key]['idref']:
                        p['cluster'] = elements_to_match[elt_key]['idref']
    # todo
    # save results id / author_key / idref
    return publications
        
