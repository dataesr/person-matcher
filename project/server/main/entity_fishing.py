import os
import requests
import pandas as pd
from project.server.main.strings import normalize2
from project.server.main.logger import get_logger


ENTITY_FISHING_SERVICE = os.getenv('ENTITY_FISHING_SERVICE')

logger = get_logger(__name__)

forbid_wiki = set(pd.read_csv('forbid_wiki.csv')['wiki_id'].to_list())

def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            logger.error(f'{func.__name__} raises an error through decorator "exception_handler".')
            logger.error(exception)
            return None
    return inner_function


def get_from_mongo(pid, myclient):
    mydb = myclient['scanr']
    collection_name = 'classifications'
    mycoll = mydb[collection_name]
    res = mycoll.find_one({'id': pid})
    if res:
        return {'classifications': res['cache']}
    return


@exception_handler
def get_entity_fishing(compute_new, publication: dict, myclient) -> dict:
    
    is_ok_to_use_cache = True

    if is_ok_to_use_cache:
        pre_computed = get_from_mongo(publication['id'], myclient)
        if pre_computed and isinstance(pre_computed.get('classifications'), list):
            return pre_computed

    if compute_new is False:
        return {}

    #logger.debug(f"compute classifications from entity fishing for {publication['id']}")
    ans = {}

    label = publication.get('label')
    title_fr, title_en, title_default = '', '', ''
    if isinstance(label, dict):
        title_fr = label.get('fr', '')
        title_en = label.get('en', '')
        title_default = label.get('default', '')
    
    keywords = publication.get('keywords')
    keywords_fr, keywords_en, keywords_default = '', '', ''
    if isinstance(keywords, dict):
        keywords_fr = ' '.join(keywords.get('fr', []))
        keywords_en = ' '.join(keywords.get('en', []))
        keywords_default = ' '.join(keywords.get('default', []))

    summary = publication.get('summary')
    summary_fr, summary_en, summary_default = '', '', ''
    if isinstance(summary, dict):
        summary_fr = summary.get('fr', '')
        summary_en = summary.get('en', '')
        summary_default = summary.get('default', '')
    
    text_fr = f"{title_fr} {keywords_fr} {summary_fr}".strip()
    text_en = f"{title_en} {keywords_en} {summary_en}".strip()
    text_default = f"{title_default} {keywords_default} {summary_default}".strip()

    classifications_fr = call_ef(text_fr, 'fr')
    classifications_en = call_ef(text_en, 'en')
    classifications_default = call_ef(text_default, 'en')

    classifications = classifications_fr
    for c in classifications_en + classifications_default:
        if c not in classifications:
            classifications.append(c)

    return classifications

def call_ef(text, lang):
    classifications = []
    if text and isinstance(text, str) and len(text)>20:
        params = {
        "language": {"lang": lang},
        'text': text,
        "mentions": [ "wikipedia"] 
        }
        if len(text.split(' '))<30:
            params['shortText'] = text
        r = requests.post(f"{ENTITY_FISHING_SERVICE}/service/disambiguate", json = params)
        res = r.json()

        global_categories = [{'label': r['category'], 'code':r['page_id'], 'reference': r['source']} for r in res.get('global_categories', []) if 'category' in r]
        wikidataIds = [{'code': r['wikidataId'], 'label': r['rawName'], 'reference': 'wikidata'} for r in res.get('entities', []) if 'wikidataId' in r]

        domains = []
        for r in res.get('entities', []):
            for d in r.get('domains', []):
                domains.append({'label': d, 'code': r['wikipediaExternalRef'], 'reference': 'wikipedia'})

        classifications += global_categories + wikidataIds + domains
    return format_classif(classifications)

def format_classif(classifications):
    domains = []
    if isinstance(classifications, list):
        for c in classifications:
            if c.get('label'):
                domain = {'label': {'default': c['label']}}
                domain['code'] = str(c.get('code'))
                if domain['code'] in forbid_wiki:
                    continue
                domain['type'] = c.get('reference')
                if domain['type'] in ['wikidata', 'sudoc']:
                    domains.append(domain)
    map_code = {}
    for d in domains:
        if 'code' in d:
            code = d.get('code')
            if code in forbid_wiki:
                continue
        else:
            code = d.get('label', {}).get('default', '')
        if isinstance(d.get('code'), str) and isinstance(d.get('label', {}).get('default'), str):
            d['id_name'] = f"{d['code']}###{d['label']['default']}"
        domain_key = normalize2(d.get('label', {}).get('default', '').lower(), remove_space=True)
        d['key_name'] = f"{domain_key}###{d['label']['default']}"
        if code not in map_code:
            map_code[code] = d
            map_code[code]['count'] = 1
            map_code[code]['naturalKey'] = domain_key
        else:
            map_code[code]['count'] += 1
    domains_with_count = list(map_code.values())
    domains_with_count = sorted(domains_with_count, key=lambda x:x['count'], reverse=True)
    return domains_with_count
