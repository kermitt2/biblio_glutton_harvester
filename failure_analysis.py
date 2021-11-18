'''

A modest postprocessing script for analyzing the generated map.jsonl file and count 
harvesting failures by domains. For every entry without pdf resource, we take the
base url of the OA link and generate a distribution of the failures by domain
in csv format. 

'''

import sys
import os
import shutil
import gzip
import json
import argparse
import time
from urllib.parse import urlparse
from tqdm import tqdm

def analyze_failure(map_jsonl, output):
    # check the overall number of entries based on the line number
    print("\ncalculating number of entries...")

    count = 0
    if map_jsonl.endswith(".gz"):
        with gzip.open(map_jsonl, 'rb') as gz:  
            while 1:
                buffer = gz.read(8192*1024)
                if not buffer: break
                count += buffer.count(b'\n') 
    else:
        with open(map_jsonl, 'rb') as jsonl:
            while 1:
                buffer = jsonl.read(8192*1024)
                if not buffer: break
                count += buffer.count(b'\n') 

    print("total of", str(count), "entries")

    nb_failed_entries = 0
    distribution = {}

    if map_jsonl.endswith(".gz"):
        with gzip.open(map_jsonl, 'rt') as gz:  
            for line in tqdm(gz, total=count):
                nb_failed_entries += process_entry(line, distribution)
    else:
        with open(map_jsonl, 'rt') as jsonl:
            for line in tqdm(jsonl, total=count):
                nb_failed_entries += process_entry(line, distribution)

    # write csv file with the distribution
    a = nb_failed_entries*100/count
    print("failure for", str(nb_failed_entries), "entries out of", str(count), "( %.2f" % a, "% )")

    with open(output, "w") as file_out:
        # Writing data to a file
        file_out.write("domain,count\n")
        for w in sorted(distribution, key=distribution.get, reverse=True):
            file_out.write(str(w) + ","+ str(distribution[w]) + "\n")

def process_entry(line, distribution):
    entry = json.loads(line)
    success = False 

    if 'pdf' in entry['resources']:
        success= True
    else:
        oa_url = entry['oa_link']
        # get base url
        url_parsing = urlparse(oa_url)
        base_url = url_parsing.netloc

        # but we just keep the ending domain
        if base_url.find(":") != -1:
            base_url = base_url[0:base_url.find(":")]
        pieces = base_url.split(".")
        base_url = pieces[-2]+"."+pieces[-1]

        if base_url in distribution:
            distribution[base_url] += 1
        else:
            distribution[base_url] = 1

    if success:
        return 0
    else:
        return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Open Access PDF harvester")
    parser.add_argument("--map", default=None, help="path to the map file (default map.jsonl) to be analyzed") 
    parser.add_argument("--output", default="failures.csv", help="where to write the result of the analysis (default failures.csv)") 

    args = parser.parse_args()

    map_jsonl = args.map
    output = args.output

    if not os.path.isfile(map_jsonl):
        print("error: the indicated path to the map file is not valid", map_jsonl)
    elif output != None and os.path.isdir(output):
        print("error: the indicated output path is not valid", output)
    else:
        start_time = time.time()

        analyze_failure(map_jsonl, output)

        runtime = round(time.time() - start_time, 3)
        print("runtime: %s seconds " % (runtime))
