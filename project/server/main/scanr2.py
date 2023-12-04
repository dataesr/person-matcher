from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object, delete_object
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.denormalize_affiliations import get_orga, get_orga_data
from project.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from project.server.main.elastic import reset_index_scanr

import pysftp
import requests
from bs4 import BeautifulSoup
import os
import json
import pymongo
import pandas as pd
from retry import retry
from dateutil import parser
from urllib import parse

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

person_id_key = 'person'

# sed -e 's/\"prizes\": \[\(.*\)\}\], \"f/f/' persons2.json > persons.json &

@retry(delay=200, tries=3)
def get_publications_for_idref(idref):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publi_meta'
    mycoll = mydb[collection_name]
    res = []
    cursor = mycoll.find({ 'authors.person' : { '$in': [idref] } })
    for r in cursor:
        del r['_id']
        res.append(r)
    cursor.close()
    myclient.close()
    return res

def export_scanr2(args):
    index_name = args.get('index')
    if args.get('new_idrefs', True):
        # writing output idrefs
        logger.debug(f'writing {MOUNTED_VOLUME}output_idrefs.csv')
        os.system(f'echo "idref" > {MOUNTED_VOLUME}output_idrefs.csv')
        cmd = f"mongoexport --forceTableScan --uri mongodb://mongo:27017/scanr --collection person_matcher_output --fields person_id --type=csv --noHeaderLine --out {MOUNTED_VOLUME}tmp.csv && cat {MOUNTED_VOLUME}tmp.csv | sort -u >> {MOUNTED_VOLUME}output_idrefs.csv"
        os.system(cmd)
        os.system(f'rm -rf {MOUNTED_VOLUME}tmp.csv')

    if args.get('reload_index_only', False) is False:
        df = pd.read_csv(f'{MOUNTED_VOLUME}/output_idrefs.csv')
        idrefs = set([k.replace('idref', '') for k in df.idref.tolist()])
        logger.debug(f'{len(idrefs)} idrefs')
        download_object('misc', 'vip.jsonl', f'{MOUNTED_VOLUME}vip.jsonl')
        input_idrefs = pd.read_json(f'{MOUNTED_VOLUME}vip.jsonl', lines=True).to_dict(orient='records')
        input_dict = {}
        for e in input_idrefs:
            input_dict[e['id'].replace('idref','')] = e
        # add extra idref 
        idrefs.update(input_dict.keys())
        ix = 0

        myclient = pymongo.MongoClient('mongodb://mongo:27017/')
        mydb = myclient['scanr']
        collection_name = 'publi_meta'
        mycoll = mydb[collection_name]
        mycoll.create_index('authors.person')
        myclient.close()
        os.system(f'rm -rf {MOUNTED_VOLUME}scanr/persons_denormalized.jsonl')
        df_orga = get_orga_data()
        for idref in idrefs:
            person = export_one_person(idref, input_dict, df_orga, ix)
            to_jsonl([person], f'{MOUNTED_VOLUME}scanr/persons_denormalized.jsonl')
            ix += 1
        os.system(f'cd {MOUNTED_VOLUME}scanr && rm -rf persons_denormalized.jsonl.gz && gzip -k persons_denormalized.jsonl')
        upload_object(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/persons_denormalized.jsonl.gz', destination='production/persons_denormalized.jsonl.gz')
    
    load_scanr_persons('/upw_data/scanr/persons_denormalized.jsonl', 'scanr-persons-'+index_name.split('-')[-1])

def clean_sudoc(idref, publications):
    sudoc_only = True
    for e in publications:
        if 'sudoc' not in e['id']:
            sudoc_only=False
            return
    logger.debug(f'clean {idref}, sudoc only')
    for sudoc_id in [e['id'] for e in publications]:
        delete_object('sudoc', f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json')


def export_one_person(idref, input_dict, df_orga, ix):
    prizes, links, externalIds = [], [], []
    if idref in input_dict:
        current_data = input_dict[idref]
        prizes = current_data.get('prizes')
        links = current_data.get('links')
        externalIds = current_data.get('externalIds')
    publications = get_publications_for_idref(f'idref{idref}')
    clean_sudoc(f'idref{idref}', publications)
    logger.debug(f'{len(publications)} publications for idref{idref} (ix={ix})')
    domains, co_authors, author_publications = [], [], []
    co_authors_id = set([])
    affiliations, names = {}, {}
    domainsCount = {}
    for p in publications:
        year = p.get('year')
        if year:
            year = str(int(year)).replace('.0', '')
        if isinstance(p.get('domains'), list):
            for d in p.get('domains', []):
                domain_key = d.get('label', {}).get('default', '').lower().strip() + ';' + d.get('code', 'nocode') + ';' + d.get('type', 'notype') 
                if domain_key not in domainsCount:
                    domainsCount[domain_key] = {'count': 0, 'domain': d}
                domainsCount[domain_key]['count'] += 1
        if isinstance(p.get('authors', []), list):
            for a in p.get('authors', []):
                if a.get(person_id_key) == 'idref'+idref:
                    author_publications.append({'publication': p['id'], 'role': a.get('role', 'author')})
                    key = None
                    if a.get('firstName') and a.get('lastName'):
                        key = f"FIRST_LAST;{a.get('firstName')};{a.get('lastName')}"
                    elif a.get('fullName'):
                        key = f"FULL;{a.get('fullName')}"
                    if key:
                        if key not in names:
                            names[key] = 1
                        else:
                            names[key] += 1
                    if isinstance(a.get('affiliations', []), list):
                        for aff in a.get('affiliations', []):
                            denormalized = get_orga(df_orga, aff)
                            if denormalized and denormalized.get('label', {}).get('default'):
                                if aff not in affiliations:
                                    affiliations[aff] = {'structure': denormalized, 'sources': [], 'sources_id': set([])}
                                if p['id'] not in affiliations[aff]['sources_id']:
                                    affiliations[aff]['sources'].append({'id': p['id'], 'year': year})
                                    affiliations[aff]['sources_id'].add(p['id'])
                                if year and len(str(year))==4:
                                    if 'endDate' not in affiliations[aff]:
                                        affiliations[aff]['endDate'] = f'{year}-12-31T00:00:00'
                                    else:
                                        affiliations[aff]['endDate'] = max(affiliations[aff]['endDate'], f'{year}-12-31T00:00:00')
                                    if 'startDate' not in affiliations[aff]:
                                        affiliations[aff]['startDate'] = f'{year}-01-01T00:00:00'
                                    else:
                                        affiliations[aff]['startDate'] = min(affiliations[aff]['startDate'], f'{year}-01-01T00:00:00')
                elif 'nnt' not in p['id'] and a.get(person_id_key) and 'idref' in a.get(person_id_key) and a.get('role') and 'aut' in a.get('role'):
                    if a[person_id_key] not in co_authors_id:
                        co_authors.append({'person': a[person_id_key], 'fullName': a.get('fullName')})
                        co_authors_id.add(a[person_id_key])
    for domain_key in domainsCount:
        domainsCount[domain_key]['domain']['count'] = domainsCount[domain_key]['count']
    for d in list(domainsCount.values()):
        domains.append(d['domain'])
    domains = sorted(domains, key=lambda e:e['count'], reverse=True)
    person = {'id': f'idref{idref}', 'coContributors': co_authors, 'publications': author_publications, 'domains': domains}
    affiliations = [a for a in list(affiliations.values()) if a.get('startDate')]
    affiliations = sorted(affiliations, key=lambda e:e.get('startDate'), reverse=True)
    for a in affiliations:
        if 'sources_id' in a:
            del a['sources_id']
    if affiliations:
        person['affiliations'] = affiliations
    if isinstance(prizes, list):
        awards = []
        for p in prizes:
            award = {}
            if p.get('prize_name'):
                award['label'] = p['prize_name']
            if p.get('prize_date'):
                try:
                    award['date'] = dateutil.parser.parse(p['prize_date']).isoformat()
                except:
                    logger.debug(f"award date not valid : {p['prize_date']}")
            if p.get('prize_url'):
                award['url'] = p['prize_url']
            if award:
                awards.append(award)
        if awards:
            person['awards'] = awards
    if isinstance(externalIds, list):
        person['externalIds'] = externalIds
        if 'idref' not in [ex.get('type') for ex in externalIds]:
            person['externalIds'].append({'type': 'idref', 'id': idref})
        for c in externalIds:
            if c['type'] == 'orcid':
                person['orcid'] = c['id']
            if c['type'] == 'id_hal':
                person['id_hal'] = c['id']
    if len(names) > 0:
        main_name = sorted(names.items(), key=lambda item: item[1], reverse=True)[0][0]
        if 'FIRST_LAST;'in main_name:
            person['firstName'] = main_name.replace('FIRST_LAST;','').split(';')[0]
            person['lastName'] = main_name.replace('FIRST_LAST;','').split(';')[1]
            person['fullName'] = f"{person['firstName']} {person['lastName']}"
        elif 'FULL;' in main_name:
            person['fullName'] = main_name.replace('FULL;','')
    if len(person.get('first_name', '')) < 4:
            try:
                idref_info = get_idref_info(idref)
                person.update(idref_info)
            except:
                pass
    return person
                       

@retry(delay=200, tries=3)
def get_idref_info(idref):
    r = requests.get(f'https://www.idref.fr/{idref}.xml').text
    soup = BeautifulSoup(r, 'lxml')
    person = {'id': f'idref{idref}'}
    #name
    name_elt = soup.find('datafield', {'tag': '200'})
    if name_elt:
        fullName = ''
        first_name_elt = name_elt.find('subfield', {'code': 'b'})
        if first_name_elt:
            first_name = first_name_elt.text
            person['firstName'] = first_name
            fullName = first_name+ ' '

        last_name_elt = name_elt.find('subfield', {'code': 'a'})
        if last_name_elt:
            last_name = last_name_elt.text
            person['lastName'] = last_name
            fullName += last_name
        fullName = fullName.strip()
        if fullName:
            person['fullName'] = fullName
    #gender
    gender_elt = soup.find('datafield', {'tag': '120'})
    if gender_elt:
        sub_elt = gender_elt.find('subfield', {'code': 'a'})
        if sub_elt and sub_elt.text == 'aa':
            person['gender'] = 'F'
        elif sub_elt and sub_elt.text == 'ba':
            person['gender'] = 'M'
    #ids
    externalIds = [{'type': 'idref', 'id': idref}]
    for id_elt in soup.find_all('datafield', {'tag': '035'}):
        code_elt = id_elt.find('subfield', {'code': '2'})
        if code_elt and code_elt.text.lower() == 'orcid':
            orcid = id_elt.find('subfield', {'code': 'a'}).text
            orcid = orcid.replace('-', '').replace(' ', '')
            orcid = orcid[0:4]+'-'+orcid[4:8]+'-'+orcid[8:12]+'-'+orcid[12:16]
            externalIds.append({'type': 'orcid', 'id': orcid})
    id_hal=None
    for id_elt in soup.find_all('datafield', {'tag': '035'}):
        code_elt = id_elt.find('subfield', {'code': '2'})
        if code_elt and code_elt.text.lower() == 'hal':
            id_hal = id_elt.find('subfield', {'code': 'a'}).text
            externalIds.append({'type': 'id_hal', 'id': id_hal})
    if externalIds:
        person['externalIds'] = externalIds
    return person

def load_scanr_persons(scanr_output_file_denormalized, index_name):
    denormalized_file=scanr_output_file_denormalized
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-persons index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={denormalized_file} --output={es_host}{index_name} --type=data --limit 500 " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)
