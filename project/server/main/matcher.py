from project.server.main.association_matcher import association_match
from project.server.main.idref_matcher import name_idref_match
from project.server.main.strings import normalize
from project.server.main.logger import get_logger


logger = get_logger(__name__)

def prepare_publications(publications):
    for p in publications:
        p['nb_authors'] = len(p.get('authors', []))
    
        entity_linked = []
        for a in p.get('authors', []):
            author_key = None
            if a.get('last_name') and a.get('first_name'):
                author_key = normalize(a.get('first_name'), remove_space=True)[0]+normalize(a.get('last_name'), remove_space=True)
            elif a.get('full_name'):
                author_key = normalize(a.get('full_name', remove_space=True))

            if author_key and len(author_key) > 4:
                a['author_key'] = author_key
                entity_linked.append(author_key)

        for other_entity in ['issns', 'keywords']:
            for elt in p.get(other_entity, []):
                entity_linked.append(normalize(elt, remove_space=True))

        entity_linked = list(set(entity_linked))
        p['entity_linked'] = entity_linked
    
    publis = sorted(publications, key=lambda p: p['nb_authors'], reverse=True)

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
                    elt_key = f"{a.get('first_name')};;;{a.get('last_name')};;;{a.get('full_name')}"
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
        if p.get('cluster') is None or 'internal' in p.get('cluster'):
            for a in p.get('authors', []):
                if a.get('author_key') == author_key:
                    elt_key = f"{a.get('first_name')};;;{a.get('last_name')};;;{a.get('full_name')}"
                    if elements_to_match[elt_key]['idref']:
                        p['cluster'] = elements_to_match[elt_key]['idref']
    # todo
    # save results id / author_key / idref
    return publications
        
