from project.server.main.strings import normalize
from project.server.main.logger import get_logger

import pandas as pd
from collections import Counter

logger = get_logger(__name__)
verbose=False

def filter_entity_linked(entities, author_key):
    res = []
    for e in entities:
        if e == author_key:
            continue
        if len(e) <= 6:
            continue
        res.append(e)
    return res

def get_main_modality(x, min_occurences_to_reach):
    cnt = Counter()
    for e in x:
        cnt[e] +=1
    top_2 = cnt.most_common(2)
    if len(top_2) == 0:
        return None
    max_occurences = top_2[0][1]
    if max_occurences < min_occurences_to_reach:
        return None
    if len(top_2)==2: # si plusieurs, le premier doit être suffisamment plus fréquent que le second
        if top_2[0][1] > top_2[1][1] * 2:
            return top_2[0][0]
    elif len(top_2)==1:
        return top_2[0][0]
    return None


def compute_entity_to_cluster_links(entity_to_cluster, publications, author_key):
    existing_clusters = []
    for p in publications:
        if p.get('person_id'):
            cluster_id = p['person_id']['id']
            if cluster_id not in existing_clusters:
                existing_clusters.append(cluster_id)
            entities = filter_entity_linked(p.get('entity_linked'), author_key)
            for e in entities:
                if e not in entity_to_cluster:
                    entity_to_cluster[e] = []
                if cluster_id not in entity_to_cluster[e] :
                    entity_to_cluster[e].append(cluster_id)
    logger.debug(f'{len(entity_to_cluster)} entities are linked to a cluster, {len(existing_clusters)} clusters in total')
    return entity_to_cluster

def apply_links(entity_to_cluster, publications, author_key):
    nb_publications_pre_identified = 0
    nb_new_publications_identified = 0
    for p in publications:
        if p.get('person_id') is not None:
            nb_publications_pre_identified += 1
            continue
        entities = filter_entity_linked(p.get('entity_linked'), author_key)
        possible_clusters = []
        for e in entities:
            if e in entity_to_cluster:
                possible_clusters += entity_to_cluster[e]
        selected_cluster = get_main_modality(possible_clusters, 2)
        if selected_cluster:
            p['person_id'] = {'id': selected_cluster, 'method': 'association'}
            nb_new_publications_identified += 1
    logger.debug(f'# publis pre-identified: {nb_publications_pre_identified} ; new identified: {nb_new_publications_identified}')
    return publications

def association_match(publications, author_key):
    entity_to_cluster = {}
    for i in range(0, 10):
        entity_to_cluster = compute_entity_to_cluster_links(entity_to_cluster, publications, author_key)
        publications = apply_links(entity_to_cluster, publications, author_key)
    return {'publications': publications, 'entity_to_cluster': entity_to_cluster} 

