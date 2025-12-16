import requests
import os
import shutil
import json

from tempfile import mkdtemp
from zipfile import ZipFile
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 128

SOURCE = 'ror'
SCHEMA_VERSION = "2.1"

def get_last_ror_dump_url():
    ROR_URL = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    response = requests.get(url=ROR_URL).json()
    ror_dump_url = response['hits']['hits'][0]['files'][-1]['links']['self']
    logger.debug(f'Last ROR dump url found: {ror_dump_url}')
    return ror_dump_url

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

