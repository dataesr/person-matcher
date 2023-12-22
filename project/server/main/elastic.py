from elasticsearch import Elasticsearch, helpers

from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.decorator import exception_handler
from project.server.main.logger import get_logger

client = None
logger = get_logger(__name__)


@exception_handler
def get_client():
    global client
    if client is None:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK))
    return client


@exception_handler
def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


@exception_handler
def update_alias(alias: str, old_index: str, new_index: str) -> None:
    es = get_client()
    logger.debug(f'updating alias {alias} from {old_index} to {new_index}')
    response = es.indices.update_aliases({
        'actions': [
            {'remove': {'index': old_index, 'alias': alias}},
            {'add': {'index': new_index, 'alias': alias}}
        ]
    })
    logger.debug(response)

def get_analyzers() -> dict:
    return {
        'light': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding'
            ]
        },
        'html_analyzer': {
          "tokenizer": "keyword",
          "char_filter": [
            "html_strip"
          ]
        }
    }

def get_filters() -> dict:
    return {
        'french_elision': {
            'type': 'elision',
            'articles_case': True,
            'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c', 'jusqu', 'quoiqu', 'lorsqu', 'puisqu']
        }
    }

@exception_handler
def reset_index_scanr(index: str) -> None:
    es = get_client()
    delete_index(index)

    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        }
    }
    
    mappings = { 'properties': {} }
    for f in ['firstName', 'lastName', 'fullName', 'label.fr', 'label.en', 'label.default', 'alias', 'leaders.firstName', 'leaders.lastName', 
            'institutions.label', 'acronym.en', 'acronym.fr', 'acronym.default', 'keywords.en', 'keywords.fr', 'keywords.default', 'domains.label.default',
            'participants.label.default']:
        mappings['properties'][f] = { 
                'type': 'text',
                'analyzer': 'light',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    for f in ['address.address', 'address.city', 'address.country', 'description.fr', 'description.en', 'description.default']: 
        mappings['properties'][f] = { 
                'type': 'text',
                'analyzer': 'light',
            }
    
    for f in ['web_content']: 
        mappings['properties'][f] = { 
                'type': 'text',
                'analyzer': 'html_analyzer',
            }

    dynamic_match = None

    if dynamic_match:
        mappings["dynamic_templates"] = [
                {
                    "objects": {
                        "match": dynamic_match,
                        "match_mapping_type": "object",
                        "mapping": {
                            "type": "nested"
                        }
                    }
                }
            ]
    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')
    else:
        logger.debug(f'ERROR !')
        logger.debug(response)

@exception_handler
def load_in_es(data: list, index: str) -> list:
    es = get_client()
    actions = [{'_index': index, '_source': datum} for datum in data]
    ix = 0
    indexed = []
    for success, info in helpers.parallel_bulk(client=es, actions=actions, chunk_size=500, request_timeout=60,
                                               raise_on_error=False):
        if not success:
            logger.debug(f'A document failed: {info}')
        else:
            indexed.append(data[ix])
        ix += 1
    logger.debug(f'{len(data)} elements imported into {index}')
    return indexed
