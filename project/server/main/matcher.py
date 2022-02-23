from project.server.main.association_matcher import association_match
from project.server.main.idref_matcher import name_idref_match
from project.server.main.strings import normalize
from project.server.main.logger import get_logger

import pymongo
import pandas as pd

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

def pre_process_publications():
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']

    df_all = pd.read_json(f'{MOUNTED_VOLUME}/test-scanr_full.jsonl', lines=True, chunksize=25000)
    for df in df_all:
        publications = df.to_dict(orient='records')
        prepare_publications(publications)

def prepare_publications(publications):
    relevant_infos = []
    # keeping and enriching only with relevant info for person matching
    for p in publications:
        new_elt = {'id': p['title_first_author']}

        new_elt['nb_authors'] = len(p.get('authors', []))
        new_elt['authors'] = []

        entity_linked = []
        for a in p.get('authors', []):
            author_key = None
            current_author = {}
            if a.get('last_name') and a.get('first_name'):
                author_key = normalize(a.get('first_name'), remove_space=True)[0]+normalize(a.get('last_name'), remove_space=True)
                current_author['last_name'] = a['last_name']
                current_author['first_name'] = a['first_name']
            elif a.get('full_name'):
                author_key = normalize(a.get('full_name', remove_space=True))
                current_author['full_name'] = a['full_name']

            if author_key and len(author_key) > 4:
                current_author['author_key'] = author_key
                entity_linked.append(author_key)

            if a.get('id'):
                entity_linked.append(a['id'])
                current_author['id'] = a['id']
            new_elt['authors'].append(current_author)

        for other_entity in ['issns', 'keywords']:
            for elt in p.get(other_entity, []):
                entity_linked.append(normalize(elt, remove_space=True))

        entity_linked = list(set(entity_linked))
        new_elt['entity_linked'] = entity_linked
    relevant_infos.append(new_elt)
    
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

    return publis

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
        
