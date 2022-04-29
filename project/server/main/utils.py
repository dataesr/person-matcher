import json
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            json.dump(entry, outfile)
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
