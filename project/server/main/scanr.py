from project.server.main.strings import normalize
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object
from project.server.main.utils import chunks, to_jsonl, to_json

import pysftp
import requests
from bs4 import BeautifulSoup
import os
import json
import pymongo
import pandas as pd
from retry import retry

logger = get_logger(__name__)
MOUNTED_VOLUME = '/upw_data/'

person_id_key = 'person'

def upload_sword(args):
    logger.debug('start sword upload')
    os.system('mkdir -p  /upw_data/logs')
    host=os.getenv('SWORD_PREPROD_HOST')
    username=os.getenv('SWORD_PREPROD_USERNAME')
    password=os.getenv('SWORD_PREPROD_PASSWORD')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host, username=username, password=password, port=2222, cnopts=cnopts, log='/upw_data/logs/logs_persons.log') as sftp:
        with sftp.cd('upload'):             # temporarily chdir to public
            sftp.put('/upw_data/scanr/persons.json')  # upload file to public/ on remote
    logger.debug('end sword upload')

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
    return res

def export_scanr(args):
    if args.get('new_idrefs', True):
        # writing output idrefs
        logger.debug(f'writing {MOUNTED_VOLUME}output_idrefs.csv')
        os.system(f'echo "idref" > {MOUNTED_VOLUME}output_idrefs.csv')
        cmd = f"mongoexport --forceTableScan --uri mongodb://mongo:27017/scanr --collection person_matcher_output --fields person_id --type=csv --noHeaderLine --out {MOUNTED_VOLUME}tmp.csv && cat {MOUNTED_VOLUME}tmp.csv | sort -u >> {MOUNTED_VOLUME}output_idrefs.csv"
        os.system(cmd)
        os.system(f'rm -rf {MOUNTED_VOLUME}tmp.csv')
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
    scanr_output_file = f'{MOUNTED_VOLUME}scanr/persons.json'
    os.system(f'rm -rf {scanr_output_file}')
    ix = 0

    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publi_meta'
    mycoll = mydb[collection_name]
    mycoll.create_index('authors.person')
    
    for idref in idrefs:
        person = export_one_person(idref, input_dict, ix)
        to_json([person], f'{MOUNTED_VOLUME}scanr/persons.json', ix)
        ix += 1
    with open(scanr_output_file, 'a') as outfile:
        outfile.write(']')
    upload_sword({})

def export_one_person(idref, input_dict, ix):
    prizes, links, externalIds = [], [], []
    if idref in input_dict:
        current_data = input_dict[idref]
        prizes = current_data.get('prizes')
        links = current_data.get('links')
        externalIds = current_data.get('externalIds')
    publications = get_publications_for_idref(f'idref{idref}')
    logger.debug(f'{len(publications)} publications for idref{idref} (ix={ix})')
    domains, co_authors, author_publications = [], [], []
    affiliations, names, keywords = {}, {}, {}
    for p in publications:
        year = p.get('year')
        if year:
            year = str(int(year))
        if isinstance(p.get('domains'), list):
            for d in p.get('domains', []):
                if d not in domains:
                    domains.append(d)
        if isinstance(p.get('keywords'), dict):
            for lang in ['default', 'fr', 'en']:
                if lang in p['keywords']:
                    if lang not in keywords:
                        keywords[lang] = []
                    for k in p['keywords'][lang]:
                        if k not in keywords[lang]:
                            keywords[lang].append(k)
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
                            if aff not in affiliations:
                                affiliations[aff] = {'structure': aff, 'sources': []}
                                affiliations[aff]['sources'].append(p['id'])
                            if year:
                                if 'endDate' not in affiliations[aff]:
                                    affiliations[aff]['endDate'] = f'{year}-12-31T00:00:00'
                                else:
                                    affiliations[aff]['endDate'] = max(affiliations[aff]['endDate'], f'{year}-12-31T00:00:00')
                                if 'startDate' not in affiliations[aff]:
                                    affiliations[aff]['startDate'] = f'{year}-01-01T00:00:00'
                                else:
                                    affiliations[aff]['startDate'] = min(affiliations[aff]['startDate'], f'{year}-01-01T00:00:00')
                elif 'nnt' not in p['id'] and a.get(person_id_key) and 'idref' in a.get(person_id_key) and a.get('role') and 'aut' in a.get('role'):
                    co_authors.append(a[person_id_key])
    person = {'id': f'idref{idref}', 'coContributors': list(set(co_authors)), 'domains': domains, 'publications': author_publications, 'keywords': keywords}
    if affiliations:
        person['affiliations'] = list(affiliations.values())
    if prizes:
        person['prizes'] = prizes
    if links:
        person['links'] = links
    if externalIds:
        person['externalIds'] = externalIds
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
    externalIds = []
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
