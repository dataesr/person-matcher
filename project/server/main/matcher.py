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

def get_id(cluster_id, input_coaut_key):
    known_ids = []
    publis_in_cluster = [p for p in publis if p['cluster'] == cluster_id]
    for p in publis_in_cluster:
        for a in p.get('authors'):
            if 'coauthor_key' in a and a['coauthor_key'] == input_coaut_key and 'id' in a:
                known_ids.append(a['id'])
    if len(set(known_ids))> 1:
        logger.debug(f"cluster {cluster_id} seems mixed up")
        return None
    if known_ids:
        return known_ids[0]

def match(publications, first_name, last_name):
    input_coauthor_key = normalize(first_name[0])+normalize(last_name)
    publis = prepare_publications(publications, input_coauthor_key) 

    entity_to_cluster = {}
    cluster_to_entities = {}

    # 1er tour basé sur les entités liées
    for p in publis:
        if not p['entity_linked']:
            continue
        entity_linked = p['entity_linked']
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
        for c in p['entity_linked']:
            clusters.append(entity_to_cluster[c])
            clusters = list(set(clusters))
        if len(clusters) == 1:
            p['cluster'] = clusters[0]
        else:
            p['cluster'] = None

    # si un identifiant est présent dans un cluster
    existing_clusters = list(cluster_to_entities.keys())
    for cluster in existing_clusters:
        idref = get_id(cluster)
        if idref:
            merge_clusters([cluster], idref)

    #3e tour / et iterations
    ix = 0
    while ix < 10:
        ix +=1
        for p in publis:
            clusters = []
            for c in p['entity_linked']:
                clusters.append(entity_to_cluster[c])
            clusters = list(set(clusters))

            if len(clusters)>1:
                matching_ids = [c for c in clusters if 'internal' not in str(c)]
                if len(matching_ids) == 1:
                    merge_clusters(clusters, matching_ids[0])
                else:
                    pass
            if clusters:
                p['cluster'] = ';'.join([str(c) for c in clusters])
            else:
                p['cluster'] = None

    logger.debug(publis)
    return publis


def prepare_publications(publications, input_coauthor_key):
    for p in publications:
        p['nb_authors'] = len(p.get('authors', []))
    publis = sorted(publications, key=lambda p: p['nb_authors'], reverse=True)
    
    entity_linked = []
    for a in p.get('authors', []):
        if a.get('last_name') and a.get('first_name'):
            co_aut = normalize(a.get('first_name'))[0]+normalize(a.get('last_name'))
            a['coauthor_key'] = co_aut
            if co_aut != input_coauthor_key and len(co_aut)> 4:
                entity_linked.append(co_aut)

    for issn in p.get('issns', []):
        entity_linked.append(issn)

    for kw in p.get('keywords', []):
        entity_linked.append(normalize(kw))

    entity_linked = list(set(entity_linked))
    p['entity_linked'] = entity_linked

    return publis
