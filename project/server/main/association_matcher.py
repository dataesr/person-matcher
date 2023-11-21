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
    return {'entity_to_cluster': entity_to_cluster, 'existing_clusters': existing_clusters}

def apply_links(entity_to_cluster, publications, author_key, idref_to_firstname):
    nb_publications_pre_identified = 0
    nb_new_publications_identified = 0
    for p in publications:
        if len(p.get('authors', [])) > 10:
            continue # risk too high to confuse co-authors
        if p.get('person_id') is not None:
            nb_publications_pre_identified += 1
            continue
        current_first_name = None
        try:
            current_first_name = normalize([a for a in p['authors'] if a.get('author_key') == author_key][0]['first_name'])
            if len(current_first_name)<=2:
                current_first_name = None
        except:
            pass

        entities = filter_entity_linked(p.get('entity_linked'), author_key)
        possible_clusters = {}
        for e in entities:
            if e in entity_to_cluster:
                for new_possible_cluster_value in entity_to_cluster[e]:
                    if new_possible_cluster_value not in possible_clusters:
                        new_possible_cluster_elt = {'cluster':new_possible_cluster_value, 'reasons': [e], 'count':1}
                    else:
                        new_possible_cluster_elt = possible_clusters[new_possible_cluster_value]
                        new_possible_cluster_elt['count'] += 1
                        new_possible_cluster_elt['reasons'].append(e)
                    possible_clusters[new_possible_cluster_value] = new_possible_cluster_elt
        sorted_possible_clusters = sorted(possible_clusters.values(), key=lambda item:item['count'], reverse=True)
        top2 = sorted_possible_clusters[0:2]
        selected_cluster = None
        if len(top2)==1:
            if top2[0]['count']>2:
                selected_cluster=top2[0]
        if len(top2)==2 and top2[0]['count']>top2[1]['count']*2:
            selected_cluster=top2[0]
        if selected_cluster and selected_cluster['cluster'] in idref_to_firstname:
            if current_first_name and current_first_name != idref_to_firstname[selected_cluster['cluster']]:
                logger.debug(f"rejection {selected_cluster['cluster']} as {current_first_name} != {idref_to_firstname[selected_cluster['cluster']]}")
                pass
            else:
                reasons = '##'.join(selected_cluster['reasons'])
                p['person_id'] = {'id': selected_cluster['cluster'], 'method': f'association;{reasons}'}
                nb_new_publications_identified += 1
    logger.debug(f'# publis pre-identified: {nb_publications_pre_identified} ; new identified: {nb_new_publications_identified}')
    return publications

def association_match(publications, author_key, idref_to_firstname):
    entity_to_cluster = {}
    for i in range(0, 10):
        entity_to_cluster_data = compute_entity_to_cluster_links(entity_to_cluster, publications, author_key)
        entity_to_cluster = entity_to_cluster_data['entity_to_cluster']
        existing_clusters = entity_to_cluster_data['existing_clusters']
        publications = apply_links(entity_to_cluster, publications, author_key, idref_to_firstname)
    return {'publications': publications, 'entity_to_cluster': entity_to_cluster} 

