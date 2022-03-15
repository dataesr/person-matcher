from project.server.main.association_matcher import association_match
from project.server.main.idref_matcher import name_idref_match
from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object
from project.server.main.utils import chunks, to_jsonl

import multiprocess as mp
import os
import json
import pymongo
import pandas as pd

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'


def get_manual_match():
    download_object('misc', 'manual_idref.json', '/upw_data/manual_idref.json')
    publi_author_dict = {}
    infos = pd.read_json('/upw_data/manual_idref.json', lines=True).to_dict(orient='records')
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
    return list(mycoll.find({'authors.author_key': author_key}))

def get_matches_for_publication(publi_ids):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'person_matcher_output'
    mycoll = mydb[collection_name]
    res = list(mycoll.find({ 'publication_id' : { '$in': publi_ids } }))
    data = {}
    for r in res:
        publi_id = r.get('publication_id')
        author_key = r.get('author_key')
        person_id = r.get('person_id')
        if publi_id and author_key and person_id:
            data[f'{publi_id};{author_key}'] = person_id
    return data

def match_all(args):
    if args.get('preprocess', True):
        pre_process_publications(args)
    author_keys = json.load(open(f'{MOUNTED_VOLUME}/author_keys.json', 'r'))
    logger.debug(f'There are {len(author_keys)} author_keys')
            
    # PARALLEL
    NB_PARALLEL = 5
    author_keys_chunks = list(chunks(lst=author_keys, n=NB_PARALLEL))
    jobs = []
    for ix, current_keys in enumerate(author_keys_chunks):
        for jx, c in enumerate(current_keys):
            p = mp.Process(target=match, args=(c, ix * NB_PARALLEL + jx))
            p.start()
            jobs.append(p)
        for p in jobs:
            p.join()

    # Not parallel
    #for author_key in author_keys:
    #    match(author_key)

    post_process_publications()

def pre_process_publications(args):
    logger.debug('dropping collection person_matcher_input')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb['person_matcher_input']
    mycoll.drop()

    manuel_matches = get_manual_match()

    df_all = pd.read_json(f'{MOUNTED_VOLUME}/test-scanr_full.jsonl', lines=True, chunksize=25000)
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

def post_process_publications():
    logger.debug('applying person matches to publications')
    final_output = f'{MOUNTED_VOLUME}/test-scanr_full_person.jsonl'
    df_all = pd.read_json(f'{MOUNTED_VOLUME}/test-scanr_full.jsonl', lines=True, chunksize=5000)
    ix = 0
    for df in df_all:
        logger.debug(f'reading {ix} chunks of publications')
        publications = df.to_dict(orient='records')
        publi_ids = [e['id'] for e in publications]
        matches = get_matches_for_publication(publi_ids)
        for p in publications:
            publi_id = p.get('publi_id')
            authors = p.get('authors')
            if not isinstance(authors, list):
                continue
            for a in authors:
                author_key = None
                if normalize(a.get('first_name'), remove_space=True) and normalize(a.get('last_name'), remove_space=True):
                    author_key = normalize(a.get('first_name'), remove_space=True)[0]+normalize(a.get('last_name'), remove_space=True)
                elif normalize(a.get('full_name'), remove_space=True):
                    author_key = normalize(a.get('full_name'), remove_space=True)
                publi_author_key = f'{publi_id};{author_key}'
                if publi_author_key in matches:
                    res = matches[publi_author_key]
                    a['id'] = res['id']
                    a['id_method'] = res['method']
        to_jsonl(publications, f'{final_output}')
        ix += 1


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
        
        affiliations = p.get('affiliations')
        if isinstance(affiliations, list) and affiliations:
            for aff in affiliations:
                for identifier in aff.get('ids', []):
                    if identifier.get('type') == 'rnsr':
                        entity_linked.append(normalize(identifier['id'], remove_space=True))
        
        classifications = p.get('classifications')
        if isinstance(classifications, list) and classifications:
            for classification in classification:
                if classification.get('reference') == 'wikidata':
                    entity_linked.append(classification.get('code'))

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

def match(author_key, idx=None):

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
    publications = association_match(publications, author_key)

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
    for p in publications:
        if p.get('person_id'):
            results.append({
                'publication_id': p['id'],
                'author_key': author_key,
                'person_id': p['person_id']['id'],
                'person_id_match_method': p['person_id']['method']
                })
    save_to_mongo_results(results, author_key)
        
