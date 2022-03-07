from project.server.main.strings import normalize
from project.server.main.logger import get_logger

import pandas as pd
from collections import Counter

logger = get_logger(__name__)

def merge_clusters(proposed_clusters, target, entity_to_cluster, cluster_to_entities):
    print(f"merging {proposed_clusters} to {target}")
    for aut in entity_to_cluster:
        if entity_to_cluster[aut] in proposed_clusters:
            entity_to_cluster[aut] = target
    for c in proposed_clusters:
        if c != target:
            del cluster_to_entities[c]

def get_main_modality(x):
    cnt = Counter()
    for e in x:
        cnt[e] +=1
    top_2 = cnt.most_common(2)
    assert(len(top_2) == 2)
    if top_2[0][1] > top_2[1][1]:
        return top_2[0][0]
    return None

def get_id(publis, cluster_id, input_coaut_key):
    known_ids = []
    publis_in_cluster = [p for p in publis if p['cluster'] == cluster_id]
    for p in publis_in_cluster:
        for a in p.get('authors'):
            if 'author_key' in a and a['author_key'] == input_coaut_key and 'id' in a:
                known_ids.append(a['id'])
    if len(set(known_ids))> 1:
        logger.debug(f"cluster {cluster_id} seems mixed up")
        return None
    if known_ids:
        return known_ids[0]

def association_match(publis, input_author_key):

    entity_to_cluster = {}
    cluster_to_entities = {}

    # 1er tour basé sur les entités liées
    for p in publis:
        if not p['entity_linked']:
            continue
        entity_linked = [e for e in p['entity_linked'] if e != input_author_key]

        current_cluster = None
        possible_clusters = []
        for entity in entity_linked:
            if entity in entity_to_cluster:
                possible_clusters.append(entity_to_cluster[entity])

        current_cluster = None
        if len(set(possible_clusters))>1:
            current_cluster = get_main_modality(possible_clusters)
        elif len(set(possible_clusters)) == 1:
            current_cluster = possible_clusters[0]
            
        if current_cluster is None:
            current_cluster = f'internal_{len(cluster_to_entities)}'  
            cluster_to_entities[current_cluster] = []
        
        for entity in entity_linked:
            if entity not in entity_to_cluster:
                entity_to_cluster[entity] = current_cluster

        cluster_to_entities[current_cluster] += entity_linked
        cluster_to_entities[current_cluster] = list(set(cluster_to_entities[current_cluster]))

    # 2e tour : application des liaisons
    for p in publis:
        clusters = []
        if not p['entity_linked']:
            continue
        entity_linked = [e for e in p['entity_linked'] if e != input_author_key]
        for c in entity_linked:
            clusters.append(entity_to_cluster[c])
            clusters = list(set(clusters))
        if len(clusters) == 1:
            p['cluster'] = clusters[0]
        else:
            p['cluster'] = None

    # si un identifiant est présent dans un cluster
    existing_clusters = list(cluster_to_entities.keys())
    for cluster in existing_clusters:
        idref = get_id(publis, cluster, input_author_key)
        if idref:
            merge_clusters([cluster], idref, entity_to_cluster, cluster_to_entities)

    #3e tour / et iterations
    ix = 0
    while ix < 10:
        ix +=1
        for p in publis:
            clusters = []
            if not p['entity_linked']:
                continue
            entity_linked = [e for e in p['entity_linked'] if e != input_author_key]
            for c in entity_linked:
                clusters.append(entity_to_cluster[c])
            clusters = list(set(clusters))

            if len(clusters)>1:
                matching_ids = [c for c in clusters if 'internal' not in str(c)]
                if len(matching_ids) == 1:
                    merge_clusters(clusters, matching_ids[0], entity_to_cluster, cluster_to_entities)
                else:
                    pass
            if clusters:
                p['cluster'] = ';'.join([str(c) for c in clusters])
            else:
                p['cluster'] = None

    for p in publis:
        if p['cluster'] and 'idref' in p['cluster'] and ';' not in p['cluster'] and 'person_id' not in p:
            p['person_id'] = {'id': p['cluster'], 'method': 'association'}
    logger.debug(publis)
    return publis


