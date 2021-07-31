'''

A small preprocessing script for unpaywall dump file. The idea is to create partitions
to distribute the processing if required and avoid repeating too many queries on the same 
domains in the same harvesting batch. 

What is done by the script:
- skip entries without PDF open access resource
- distribute the entries with open access resource in n bins/files
- avoid concentrations/succession of resources from the same domain in the bins

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

def create_partition(unpaywall, output=None, nb_bins=10):
    # check the overall number of entries based on the line number
    print("\ncalculating number of entries...")
    '''
    count = 0
    with gzip.open(unpaywall, 'rb') as gz:  
        while 1:
            buffer = gz.read(8192*1024)
            if not buffer: break
            count += buffer.count(b'\n')
    '''

    count = 126388740
    
    print("total of", str(count), "entries")

    nb_oa_entries = 0

    # prepare the n bins files
    nbins_files = []
    basename = os.path.splitext(os.path.basename(unpaywall))[0]
    for n in range(nb_bins):
        if output == None:            
            dirname = os.path.dirname(unpaywall)
            out_path = os.path.join(dirname, basename + "_" + str(n) + ".jsonl.gz")
        else:
            out_path = os.path.join(output, basename + "_" + str(n) + ".jsonl.gz")
        f = gzip.open(out_path, 'wt')
        nbins_files.append(f)

    gz = gzip.open(unpaywall, 'rt')
    position = 0
    current_bin = 0
    for line in tqdm(gz, total=count):
        entry = json.loads(line)

        if 'best_oa_location' in entry:
            if entry['best_oa_location'] is not None:
                if 'url_for_pdf' in entry['best_oa_location']:
                    pdf_url = entry['best_oa_location']['url_for_pdf']
                    if pdf_url is not None:
                        # add the line in the selected bin
                        nbins_files[current_bin].write(line)

                        current_bin += 1
                        if current_bin == nb_bins:
                            current_bin = 0

                        nb_oa_entries += 1

        position += 1

    gz.close()

    for n in range(nb_bins):
        nbins_files[n].close()

    print(str(nb_bins), " files generated, with a total of ", str(nb_oa_entries), "OA entries with PDF URL")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Open Access PDF harvester")
    parser.add_argument("--unpaywall", default=None, help="path to the Unpaywall dataset (gzipped)") 
    parser.add_argument("--output", help="where to write the pre-processed files (default along with the Unpaywall input file)") 
    parser.add_argument("--n", type=int, default="10", help="number of bins for partitioning the unpaywall entries") 

    args = parser.parse_args()

    unpaywall = args.unpaywall
    output = args.output
    nb_bins = args.n

    if unpaywall == None:
        print("error: the path to the Unpaywall file has not been specified")
    elif not os.path.isfile(unpaywall):
        print("error: the indicated path to the Unpaywall file is not valid", unpaywall)
    elif output != None and not os.path.isdir(output):
        print("error: the indicated output path is not valid", output)
    else:
        start_time = time.time()

        create_partition(unpaywall, output, nb_bins)

        runtime = round(time.time() - start_time, 3)
        print("runtime: %s seconds " % (runtime))
