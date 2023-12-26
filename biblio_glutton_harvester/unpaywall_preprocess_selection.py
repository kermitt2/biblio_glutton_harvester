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

        # add DOI not present in CrossRef/Unpaywall
        for doi in dois:
            entry = create_entry_from_DOI(doi)
            if entry != None:
                nb_entries += 1
                json_string = json.dumps(entry)
                output_file.write(json_string)
                output_file.write("\n")

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
            line = line.strip()
            if line.startswith("https://doi.org/"):
                line = line.replace("https://doi.org/", "")
            dois.add(line)
    return dois


def create_entry_from_DOI(doi):
    """
    Certain DOI are not present in CrossRef and Unpaywall, but can be mapped to full texts,
    this is the case for arXiv DOIs. 
    Unfortunately those DOI are used now more and more as normal crossref DOI, but will not be
    resolved by CrossRef, so ad hoc mapping are necessary. 

    TODO: extend with other similar sad DOI if relevant

    Note: bioRxiv DOI are normal CrossRef DOI !
    """
    if doi.startswith("10.48550/arxiv"):
        # arXiv has special DOI in the form: "10.48550/arxiv.", followed by arXiv identifier
        entry = create_entry_template(doi, True)
        local_url = doi.replace("10.48550/arxiv.", "https://arxiv.org/pdf/")
        local_url_landing_page = doi.replace("10.48550/arxiv.", "https://arxiv.org/abs/")
        oa_location = create_oa_location_template(local_url+".pdf", "arXiv", local_url+".pdf", local_url_landing_page)
        entry["oa_locations"] = []
        entry["oa_locations"].append(oa_location)
        entry["best_oa_location"] = oa_location
        entry["first_oa_location"] = oa_location
        return entry
    else: 
        return None


def create_entry_template(doi, is_oa):
    '''
    As of Decembre 2023 Unpaywall format, it looks like this:

    {
        "doi": "10.18653/v1/2023.acl-short.82", 
        "year": 2023, 
        "genre": "proceedings-article", 
        "is_oa": true, 
        "title": "Controllable Mixed-Initiative Dialogue Generation through Prompting", 
        "doi_url": "https://doi.org/10.18653/v1/2023.acl-short.82", 
        "updated": "2023-08-05T06:24:57.421378", 
        "oa_status": "hybrid", 
        "publisher": "Association for Computational Linguistics", 
        "z_authors": [{"given": "Maximillian", "family": "Chen", "sequence": "first"}, {"given": "Xiao", "family": "Yu", "sequence": "additional"}, {"given": "Weiyan", "family": "Shi", "sequence": "additional"}, {"given": "Urvi", "family": "Awasthi", "sequence": "additional"}, {"given": "Zhou", "family": "Yu", "sequence": "additional"}], 
        "is_paratext": false, 
        "journal_name": "Proceedings of the 61st Annual Meeting of the Association for Computational Linguistics (Volume 2: Short Papers)", 
        "oa_locations": [
                {"url": "https://aclanthology.org/2023.acl-short.82.pdf", "pmh_id": null, "is_best": true, "license": "cc-by", "oa_date": "2023-01-01", "updated": "2023-08-05T06:23:59.255925", "version": "publishedVersion", "evidence": "open (via page says license)", "host_type": "publisher", "endpoint_id": null, "url_for_pdf": "https://aclanthology.org/2023.acl-short.82.pdf", "url_for_landing_page": "https://doi.org/10.18653/v1/2023.acl-short.82", "repository_institution": null}, 
                {"url": "https://arxiv.org/pdf/2305.04147", "pmh_id": "oai:arXiv.org:2305.04147", "is_best": false, "license": null, "oa_date": "2023-05-09", "updated": "2023-05-11T19:04:59.417689", "version": "submittedVersion", "evidence": "oa repository (via OAI-PMH title and first author match)", "host_type": "repository", "endpoint_id": "ca8f8d56758a80a4f86", "url_for_pdf": "https://arxiv.org/pdf/2305.04147", "url_for_landing_page": "https://arxiv.org/abs/2305.04147", "repository_institution": "Cornell University - arXiv"}
            ], 
        "data_standard": 2, 
        "journal_is_oa": false, 
        "journal_issns": null, 
        "journal_issn_l": null, 
        "published_date": "2023-01-01", 
        "best_oa_location": {"url": "https://aclanthology.org/2023.acl-short.82.pdf", "pmh_id": null, "is_best": true, "license": "cc-by", "oa_date": "2023-01-01", "updated": "2023-08-05T06:23:59.255925", "version": "publishedVersion", "evidence": "open (via page says license)", "host_type": "publisher", "endpoint_id": null, "url_for_pdf": "https://aclanthology.org/2023.acl-short.82.pdf", "url_for_landing_page": "https://doi.org/10.18653/v1/2023.acl-short.82", "repository_institution": null}, 
        "first_oa_location": {"url": "https://aclanthology.org/2023.acl-short.82.pdf", "pmh_id": null, "is_best": true, "license": "cc-by", "oa_date": "2023-01-01", "updated": "2023-08-05T06:23:59.255925", "version": "publishedVersion", "evidence": "open (via page says license)", "host_type": "publisher", "endpoint_id": null, "url_for_pdf": "https://aclanthology.org/2023.acl-short.82.pdf", "url_for_landing_page": "https://doi.org/10.18653/v1/2023.acl-short.82", "repository_institution": null}, 
        "journal_is_in_doaj": false, 
        "has_repository_copy": true, 
        "oa_locations_embargoed": []
    }

    In the created entry, we only keep what will be used by the harvester. 
    '''
    result = {}
    result["doi"] = doi
    result["is_oa"] = is_oa
    result["doi_url"] = "https://doi.org/"+doi
    return result

def create_oa_location_template(url, license, url_for_pdf, url_for_landing_page):
    '''
    As of Decembre 2023 Unpaywall format, it looks like this:

    {
        "url": "https://aclanthology.org/2023.acl-short.82.pdf", 
        "pmh_id": null, 
        "is_best": true, 
        "license": "cc-by", 
        "oa_date": "2023-01-01", 
        "updated": "2023-08-05T06:23:59.255925", 
        "version": "publishedVersion", 
        "evidence": "open (via page says license)", 
        "host_type": "publisher", 
        "endpoint_id": null, 
        "url_for_pdf": "https://aclanthology.org/2023.acl-short.82.pdf", 
        "url_for_landing_page": "https://doi.org/10.18653/v1/2023.acl-short.82", 
        "repository_institution": null
    }

    In the created entry, we only keep what will be used by the harvester. 
    '''
    result = {}
    result["url"] = url
    result["license"] = license
    result["url_for_pdf"] = url_for_pdf
    result["url_for_landing_page"] = url_for_landing_page
    return result

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
