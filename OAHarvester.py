import boto3
import botocore
import sys
import os
import shutil
import gzip
import json
import pickle
import lmdb
import uuid
import subprocess
import argparse
import time
import S3
from concurrent.futures import ThreadPoolExecutor
import subprocess

import numpy
import math

map_size = 100 * 1024 * 1024 * 1024 
shuffle_range = math.pow(10, 6)# we will consider 1million entry for the shuffle, but there are more

'''
This version uses the standard ThreadPoolExecutor for parallelizing the download/processing/upload processes. 
Given the limits of ThreadPoolExecutor (input stored in memory, blocking Executor.map until the whole input
is processed and output stored in memory until all input is consumed), it works with batches of PDF of a size 
indicated in the config.json file (default is 100 entries). We are moving from first batch to the second one 
only when the first is entirely processed. 

'''
class OAHarverster(object):

    def __init__(self, config_path='./config.json'):
        self.config = None
        
        # standard lmdb environment for storing biblio entries by uuid
        self.env = None

        # lmdb environment for storing mapping between doi and uuid
        self.env_doi = None

        # lmdb environment for keeping track of failures
        self.env_fail = None

        self._load_config(config_path)
        self._init_lmdb()

        self.s3 = None
        if self.config["bucket_name"] is not None and len(self.config["bucket_name"]) is not 0:
            self.s3 = S3.S3(self.config)

    def _load_config(self, path='./config.json'):
        """
        Load the json configuration 
        """
        config_json = open(path).read()
        self.config = json.loads(config_json)

    def _init_lmdb(self):
        # open in write mode
        envFilePath = os.path.join(self.config["data_path"], 'entries')
        self.env = lmdb.open(envFilePath, map_size=map_size)

        envFilePath = os.path.join(self.config["data_path"], 'doi')
        self.env_doi = lmdb.open(envFilePath, map_size=map_size)

        envFilePath = os.path.join(self.config["data_path"], 'fail')
        self.env_fail = lmdb.open(envFilePath, map_size=map_size)

    def harvestUnpaywall(self, filepath):   
        """
        Main method, use the Unpaywall dataset for getting pdf url for Open Access resources, 
        download in parallel PDF, generate thumbnails, upload resources on S3 and update
        the json description of the entries
        """
        batch_size_pdf = self.config['batch_size']
        # batch size for lmdb commit
        batch_size_lmdb = 10 
        n = 0
        i = 0
        urls = []
        entries = []
        filenames = []
        
        # init lmdb transactions
        #txn = self.env.begin(write=True)
        #txn_doi = self.env_doi.begin(write=True)
        #txn_fail = self.env_fail.begin(write=True)
        X = numpy.random.randint(0, shuffle_range, size=batch_size_pdf)# for example we harvest 1000 entry
        gz = gzip.open(filepath, 'rt')
        for line in gz:
            
            if j >= shuffle_range:
                # new shuffle
                j = 0
                X = numpy.random.randint(0, shuffle_range, size=batch_size_pdf)
                
            if j not in X:
                j += 1
                continue
            j += 1
            
            if n >= 1000:
                break
            '''
            if n != 0 and n % batch_size_lmdb == 0:
                txn.commit()
                txn = self.env.begin(write=True)

                txn_doi.commit()
                txn_doi = self.env_doi.begin(write=True)

                txn_fail.commit()
                txn_fail = self.env_fail.begin(write=True)
            '''

            if i == batch_size_pdf:
                self.processBatch(urls, filenames, entries)#, txn, txn_doi, txn_fail)
                # reinit
                i = 0
                urls = []
                entries = []
                filenames = []
                n += batch_size_pdf

            # one json entry per line
            entry = json.loads(line)
            doi = entry['doi']

            # check if the entry has already been processed
            if self.getUUIDByDoi(doi) is not None:
                continue

            if 'best_oa_location' in entry:
                if entry['best_oa_location'] is not None:
                    if 'url_for_pdf' in entry['best_oa_location']:
                        pdf_url = entry['best_oa_location']['url_for_pdf']
                        #if pdf_url is not None and pdf_url.endswith('.pdf'):
                        if pdf_url is not None:    
                            print(pdf_url)
                            urls.append(pdf_url)

                            entry['id'] = str(uuid.uuid4())
                            entries.append(entry)
                            filenames.append(os.path.join(self.config["data_path"], entry['id']+".pdf"))
                            i += 1
            
        gz.close()

        # we need to process the latest incomplete batch (if not empty)
        if len(urls) >0:
            self.processBatch(urls, filenames, entries)#, txn, txn_doi, txn_fail)
            n += len(urls)

        print("total entries:", n)

    def processBatch(self, urls, filenames, entries):#, txn, txn_doi, txn_fail):
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(download, urls, filenames, entries)

        # LMDB write transaction must be performed in the thread that created the transaction, so
        # we need to have that out of the paralell process
        entries = []
        for result in results:
            if result[0] is None or result[0] == "0":
                local_entry = result[1]
                
                #update DB
                txn = self.env.begin(write=True)
                txn.put(local_entry['id'].encode(encoding='UTF-8'), _serialize_pickle(local_entry)) 
                txn.commit()

                print(" update txn_doi")
                txn_doi = self.env_doi.begin(write=True)
                txn_doi.put(local_entry['doi'].encode(encoding='UTF-8'), local_entry['id'].encode(encoding='UTF-8'))
                txn_doi.commit()

                #self.manageFiles(local_entry)
                entries.append(local_entry)
            else:
                print(" error: " + result[0])
                local_entry = result[1]
                
                #update DB
                txn = self.env.begin(write=True)
                txn.put(local_entry['id'].encode(encoding='UTF-8'), _serialize_pickle(local_entry))  
                txn.commit()

                txn_doi = self.env_doi.begin(write=True)
                txn_doi.put(local_entry['doi'].encode(encoding='UTF-8'), local_entry['id'].encode(encoding='UTF-8'))
                txn_doi.commit()

                txn_fail = self.env_fail.begin(write=True)
                txn_fail.put(local_entry['id'].encode(encoding='UTF-8'), result[0].encode(encoding='UTF-8'))
                txn_fail.commit()

                # if an empty pdf file is present, we clean it
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)

        # finally we can parallelize the thumbnail/upload/file cleaning steps for this batch
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(self.manageFiles, entries)


    def processBatchReprocess(self, urls, filenames, entries):#, txn, txn_doi, txn_fail):
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(download, urls, filenames, entries)
        
        # LMDB write transactions in the thread that created the transaction
        entries = []
        for result in results: 
            if result[0] is None or result[0] == "0":
                local_entry = result[1]
                entries.append(local_entry)
                # remove the entry in fail, as it is now sucessful
                with self.env_fail.begin(write=True) as txn_fail2:
                    txn_fail2.delete(local_entry['id'].encode(encoding='UTF-8'))
                    txn_fail2.commit()
            else:
                # still an error
                local_entry = result[1]
                # if an empty pdf file is present, we clean it
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)

        # finally we can parallelize the thumbnail/upload/file cleaning steps for this batch
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(self.manageFiles, entries)


    def getUUIDByDoi(self, doi):
        txn = self.env_doi.begin()
        return txn.get(doi.encode(encoding='UTF-8'))

    def manageFiles(self, local_entry):
        local_filename = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
        # generate thumbnails
        generate_thumbnail(local_filename)
        
        dest_path = generateS3Path(local_entry['id'])
        thumb_file_small = local_filename.replace('.pdf', '-thumb-small.png')
        thumb_file_medium = local_filename.replace('.pdf', '-thumb-medium.png')
        thumb_file_large = local_filename.replace('.pdf', '-thumb-large.png')

        if self.s3 is not None:
            # upload to S3 
            # upload is already in parallel for individual file (with parts)
            # so we don't further upload in parallel at the level of the files
            self.s3.upload_file_to_s3(local_filename, dest_path, storage_class='ONEZONE_IA')
            
            if os.path.isfile(thumb_file_small):
                self.s3.upload_file_to_s3(thumb_file_small, dest_path, storage_class='ONEZONE_IA')

            if os.path.isfile(thumb_file_medium): 
                self.s3.upload_file_to_s3(thumb_file_medium, dest_path, storage_class='ONEZONE_IA')
            
            if os.path.isfile(thumb_file_large): 
                self.s3.upload_file_to_s3(thumb_file_large, dest_path, storage_class='ONEZONE_IA')
        else:
            # save under local storate indicated by data_path in the config json
            try:
                local_dest_path = os.path.join(self.config["data_path"], dest_path)
                os.makedirs(os.path.dirname(local_dest_path), exist_ok=True)
                shutil.copyfile(local_filename, os.path.join(local_dest_path, local_entry['id']+".pdf"))

                thumb_file_small = local_filename.replace('.pdf', '-thumb-small.png')
                shutil.copyfile(thumb_file_small, os.path.join(local_dest_path, local_entry['id']+"-thumb-small.png"))

                thumb_file_medium = local_filename.replace('.pdf', '-thumb-medium.png')
                shutil.copyfile(thumb_file_medium, os.path.join(local_dest_path, local_entry['id']+"-thumb-medium.png"))

                thumb_file_large = local_filename.replace('.pdf', '-thumb-large.png')
                shutil.copyfile(thumb_file_large, os.path.join(local_dest_path, local_entry['id']+"-thumb-larger.png"))

            except IOError as e:
                print("invalid path", str(e))       

        # clean pdf and thumbnail files
        try:
            os.remove(local_filename)
            if os.path.isfile(thumb_file_small): 
                os.remove(thumb_file_small)
            if os.path.isfile(thumb_file_medium): 
                os.remove(thumb_file_medium)
            if os.path.isfile(thumb_file_large): 
                os.remove(thumb_file_large)
        except IOError as e:
            print("temporary file cleaning failed:", str(e))       


    def reprocessFailed(self):
        """
        Retry to access OA resources stored in the fail lmdb
        """
        batch_size_pdf = self.config['batch_size']
        # batch size for lmdb commit
        batch_size_lmdb = 100 
        n = 0
        i = 0
        urls = []
        entries = []
        filenames = []
        
        # init lmdb transactions
        txn = self.env.begin(write=True)
        txn_fail = self.env_fail.begin(write=True)

        nb_fails = txn_fail.stat()['entries']
        nb_total = txn.stat()['entries']
        print("number of failed entries with OA link:", nb_fails, "out of", nb_total, "entries")

        # iterate over the fail lmdb
        cursor = txn.cursor()
        for key, value in cursor:
            if i == batch_size_pdf:
                self.processBatchReprocess(urls, filenames, entries)#, txn, txn_doi, txn_fail)
                # reinit
                i = 0
                urls = []
                entries = []
                filenames = []
                n += batch_size_pdf

            with self.env_fail.begin() as txn_f:
                value_error = txn_f.get(key)
                if value_error is None:
                   continue

            local_entry = _deserialize_pickle(value)
            pdf_url = local_entry['best_oa_location']['url_for_pdf']  
            print(pdf_url)
            urls.append(pdf_url)
            entries.append(local_entry)
            filenames.append(os.path.join(self.config["data_path"], local_entry['id']+".pdf"))
            i += 1

        # we need to process the latest incomplete batch (if not empty)
        if len(urls)>0:
            self.processBatchReprocess(urls, filenames, entries)#, txn, txn_doi, txn_fail)

    def dump(self, dump_file):
        # init lmdb transactions
        txn = self.env.begin(write=True)
        
        nb_total = txn.stat()['entries']
        print("number of entries with OA link:", nb_total)

        with open(dump_file,'w') as file_out:
            # iterate over the fail lmdb
            cursor = txn.cursor()
            for key, value in cursor:
                if txn.get(key) is None:
                    continue
                local_entry = _deserialize_pickle(txn.get(key))
                file_out.write(json.dumps(local_entry))
                file_out.write("\n")

    def reset(self):
        """
        Remove the local lmdb keeping track of the state of advancement of the harvesting and
        of the failed entries
        """
        # close environments
        self.env.close()
        self.env_doi.close()
        self.env_fail.close()

        envFilePath = os.path.join(self.config["data_path"], 'entries')
        shutil.rmtree(envFilePath)

        envFilePath = os.path.join(self.config["data_path"], 'doi')
        shutil.rmtree(envFilePath)

        envFilePath = os.path.join(self.config["data_path"], 'fail')
        shutil.rmtree(envFilePath)

        # re-init the environments
        self._init_lmdb()

        # clean any possibly remaining tmp files (.pdf and .png)
        for f in os.listdir(self.config["data_path"]):
            if f.endswith(".pdf") or f.endswith(".png") :
                os.remove(os.path.join(self.config["data_path"], f))

    def diagnostic(self):
        """
        Print a report on failures stored during the harvesting process
        """
        txn = self.env.begin(write=True)
        txn_fail = self.env_fail.begin(write=True)
        nb_fails = txn_fail.stat()['entries']
        nb_total = txn.stat()['entries']
        print("number of failed entries with OA link:", nb_fails, "out of", nb_total, "entries")

def _serialize_pickle(a):
    return pickle.dumps(a)

def _deserialize_pickle(serialized):
    return pickle.loads(serialized)

def download(url, filename, entry):
    #cmd = "wget -c --quiet" + " -O " + filename + ' --connect-timeout=10 --waitretry=10 ' + \
    cmd = "wget -c --quiet" + " -O " + filename + '  --timeout=2 --waitretry=0 --tries=5 --retry-connrefused ' + \
        '--header="User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0" ' + \
        '--header="Accept: application/pdf, text/html;q=0.9,*/*;q=0.8" --header="Accept-Encoding: gzip, deflate" ' + \
        '"' + url + '"'
        #'--header="Referer: https://www.google.com"' +
        #' --random-wait' +
    print(cmd)
    try:
        result = subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)
        print("e.output", e.output)
        if  e.output is not None and e.output.startswith('error: {'):
            error = json.loads(e.output[7:]) # Skip "error: "
            print("error code:", error['code'])
            print("error message:", error['message'])
            result = error['message']
        else:
            result = e.returncode
    return str(result), entry

def generate_thumbnail(pdfFile):
    """
    Generate a PNG thumbnails (3 different sizes) for the front page of a PDF. 
    Use ImageMagick for this.
    """
    thumb_file = pdfFile.replace('.pdf', '-thumb-small.png')
    cmd = 'convert -quiet -density 200 -thumbnail x150 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)

    thumb_file = pdfFile.replace('.pdf', '-thumb-medium.png')
    cmd = 'convert -quiet -density 200 -thumbnail x300 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)

    thumb_file = pdfFile.replace('.pdf', '-thumb-large.png')
    cmd = 'convert -quiet -density 200 -thumbnail x500 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)

def generateS3Path(filename):
    '''
    Convert a file name into a path with file prefix as directory paths:
    123456789 -> 12/34/56/123456789
    '''
    return filename[:2] + '/' + filename[2:4] + '/' + filename[4:6] + "/" + filename[6:8] + "/"

def test():
    harvester = OAHarverster()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "OA PDF harvester")
    parser.add_argument("--unpaywall", default=None, help="path to the Unpaywall dataset (gzipped)") 
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    parser.add_argument("--dump", default="dump.json", help="Write all JSON entries having a sucessful OA link with their UUID") 
    parser.add_argument("--reprocess", action="store_true", help="Reprocessed failed entries with OA link") 
    parser.add_argument("--reset", action="store_true", help="Ignore previous processing states, and re-init the harvesting process from the beginning") 
    
    
    args = parser.parse_args()

    unpaywall = args.unpaywall
    config_path = args.config
    reprocess = args.reprocess
    reset = args.reset
    dump = args.dump

    harvester = OAHarverster(config_path=config_path)

    if reset:
        harvester.reset()

    start_time = time.time()

    if reprocess:
        harvester.reprocessFailed()
        harvester.diagnostic()
    elif unpaywall is not None: 
        harvester.harvestUnpaywall(unpaywall)
        harvester.diagnostic()

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))

    if dump is not None:
        harvester.dump(dump)
