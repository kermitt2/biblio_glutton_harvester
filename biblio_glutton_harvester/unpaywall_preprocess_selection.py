'''

A small preprocessing script for unpaywall dump file. The goal is to produce a subset
of the full dump restricted to a selection of DOI. A set of DOI is given by a file with 
one DOI per line. 

'''

import sys
import os
import shutil
import gzip
import json
import argparse
import time
from random import randint
from tqdm import tqdm

def create_selection(unpaywall, dois, output=None):
    nb_entries = 0

    if output == None:
        output = "output.json.gz"
    
    with gzip.open(output, 'wt') as output_file: 
        gz = gzip.open(unpaywall, 'rt')
        position = 0
        current_bin = 0
        for line in tqdm(gz, total=len(dois)):
            entry = json.loads(line)
            if 'doi' in entry:
                if entry['doi'] in dois:
                    nb_entries += 1
                    json_string = json.dumps(entry)
                    output_file.write(json_string)
                    output_file.write("\n")

                    if nb_entries == len(dois):
                        break
        gz.close()

def load_dois(input):
    """
    Load a list of DOI. DOI are loaded in memory in a set, which should be okay even for several ten millions
    and ensure constant fast look-up.
    """
    dois = set()
    with open(input) as file:
        for line in file:
            dois.add(line.strip())
    return dois


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Open Access PDF harvester")
    parser.add_argument("--unpaywall", default=None, help="path to the Unpaywall dataset (gzipped)") 
    parser.add_argument("--dois", default=None, help="path to the list of DOIs to be used to create the Unpaywall subset") 
    parser.add_argument("--output", help="where to write the subset Unpaywall file, a .json.gz extension file") 

    args = parser.parse_args()

    unpaywall = args.unpaywall
    output = args.output
    dois_path = args.dois

    if unpaywall == None:
        print("error: the path to the Unpaywall file has not been specified")
    elif dois_path == None:
        print("error: the path to the selected DOIs is not specified")
    elif not os.path.isfile(unpaywall):
        print("error: the indicated path to the Unpaywall file is not valid", unpaywall)
    elif not os.path.isfile(dois_path):
        print("error: the indicated path to the DOIs selection file is not valid", dois_path)
    elif output== None:
        print("error: the indicated output path is not valid", output)
    else:
        start_time = time.time()

        dois = load_dois(dois_path)
        if len(dois)>0:
            create_selection(unpaywall, dois, output)

        runtime = round(time.time() - start_time, 3)
        print("runtime: %s seconds " % (runtime))
