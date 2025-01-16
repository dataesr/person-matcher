import json
import pandas as pd
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_all_manual_matches():
    old_matches = pd.read_csv('manual_matches.csv.gz')
    new_matches = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vRtJvpjh4ySiniYVzgUYpGQVQEuNY7ZOpqPbi3tcyRfKiBaLnAgYziQgecX_kvwnem3fr0M34hyCTFU/pub?gid=1281340758&single=true&output=csv')
    matches = pd.concat([old_matches, new_matches])
    return matches

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def clean_json(elt):
    if isinstance(elt, dict):
        keys = list(elt.keys()).copy()
        for f in keys:
            if (not elt[f] == elt[f]) or (elt[f] is None):
                del elt[f]
            else:
                elt[f] = clean_json(elt[f])
    elif isinstance(elt, list):
        for ix, k in enumerate(elt):
            elt[ix] = clean_json(elt[ix])
    return elt

def clean_json_old(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')

def to_json(input_list, output_file, ix):
    if ix == 0:
        mode = 'w'
    else:
        mode = 'a'
    with open(output_file, mode) as outfile:
        if ix == 0:
            outfile.write('[')
        for jx, entry in enumerate(input_list):
            if ix + jx != 0:
                outfile.write(',\n')
            json.dump(entry, outfile)
