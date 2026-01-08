import requests
import os
import shutil
import json
import pandas as pd
from retry import retry

from tempfile import mkdtemp
from zipfile import ZipFile
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 128

SOURCE = 'ror'
SCHEMA_VERSION = "2.1"

@retry(delay=200, tries=3)
def get_last_ror_dump_url():
    ROR_URL = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    response = requests.get(url=ROR_URL).json()
    ror_dump_url = response['hits']['hits'][0]['files'][-1]['links']['self']
    logger.debug(f'Last ROR dump url found: {ror_dump_url}')
    return ror_dump_url

@retry(delay=200, tries=3)
def dump_ror_data() -> list:
    ROR_DUMP_URL = get_last_ror_dump_url()
    logger.debug(f'download ROR from {ROR_DUMP_URL}')
    ror_downloaded_file = 'ror_data_dump.zip'
    ror_unzipped_folder = mkdtemp()
    response = requests.get(url=ROR_DUMP_URL, stream=True)

    with open(file=ror_downloaded_file, mode='wb') as file:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            file.write(chunk)
    with ZipFile(file=ror_downloaded_file, mode='r') as file:
        file.extractall(ror_unzipped_folder)

    found_version = False
    for data_file in os.listdir(ror_unzipped_folder):
        if data_file.endswith('.json'):
            with open(f'{ror_unzipped_folder}/{data_file}', 'r') as file:
                data = json.load(file)
                # Check schema version
                if SCHEMA_VERSION == data[0].get("admin", {}).get("last_modified", {}).get("schema_version"):
                    found_version = True
                    break

    os.remove(path=ror_downloaded_file)
    shutil.rmtree(path=ror_unzipped_folder)

    if not found_version:
        logger.debug(f"Error: ROR schema version {SCHEMA_VERSION} not found in {ROR_DUMP_URL}")
        data = []
    os.system('mkdir -p /upw_data/scanr/orga_ref')
    os.system(f'rm -rf /upw_data/scanr/orga_ref/ror.jsonl')
    to_jsonl(data, f'/upw_data/scanr/orga_ref/ror.jsonl')
    os.system(f'cd /upw_data/scanr/orga_ref && rm -rf ror.jsonl.gz && gzip -k ror.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/orga_ref/ror.jsonl.gz', destination=f'production/ror.jsonl.gz')

def format_ror(ror_ids, existing_rors):
    input_id_set_keep = set([r.lower().strip() for r in ror_ids])
    input_id_set_skip = set([r.lower().strip() for r in existing_rors])
    df_ror = pd.read_json('/upw_data/scanr/orga_ref/ror.jsonl', lines=True)
    ror_formatted = []
    for e in df_ror.to_dict(orient='records'):
        new_elt = {'id': e['id'].split('/')[-1]}
        if new_elt['id'] not in input_id_set_keep:
            continue
        if new_elt['id'] in input_id_set_skip:
            continue
        new_elt['externalIds'] = [{'id': e['id'], 'type': 'ror'}]
        for k in e['external_ids']:
            if isinstance(k.get('all'), list):
                for g in k['all']:
                    new_k = {'id': g, 'type': k['type']}
                    if new_k not in new_elt['externalIds']:
                        new_elt['externalIds'].append(new_k)
        # startDate
        if isinstance(e.get('established'), int):
            new_elt['startDate'] = str(e['established'])+'-01-01T00:00:00'
        if new_elt.get('startDate'):
            new_elt['creationYear'] = int(new_elt['startDate'][0:4])
        # status
        if e.get('status')=='active':
            new_elt['status']='active'
        else:
            new_elt['status'] = 'old'
        # endDate
        # name
        new_elt['label'] = {}
        for n in e.get('names', []):
            if 'ror_display' in n.get('types', []):
                new_elt['label']['default'] = n['value']
            if 'label' in n.get('types', []) and 'lang' in n:
                new_elt['label'][n['lang']] = n['value']
        # kind
        if 'company' in e.get('types', []):
            new_elt['kind'] = ['Secteur privé'] 
        else:
            new_elt['kind'] = ['Secteur public'] 
        #address
        address = {'main': True}
        if len(e.get('locations'), []) > 1:
            if isinstance(e['locations'][0].get('geonames_details'), dict):
                geo_details = e['locations'][0].get('geonames_details')
                if 'name' in geo_details:
                    address['city'] = geo_details['name']
                if 'country_name' in geo_details:
                    address['country'] = geo_details['country_name']
                if 'country_code' in geo_details:
                    address['iso2'] = geo_details['country_code']
                    if address.get('iso2') == 'FR':
                        new_elt['isFrench'] = True
                if 'lat' in geo_details and 'lng' in geo_details:
                    address['gps'] = {'lat': geo_details['lat'], 'lon': geo_details['lng']}
        new_elt['address'] = [address]
        # website
        if e.get('links'):
            new_elt['links'] = e['websites']
            for k in new_elt['links']:
                k['url'] = k['value']
        ror_formatted.append(new_elt)
    return ror_formatted


