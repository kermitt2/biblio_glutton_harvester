import boto3
import botocore
import sys
import os
import shutil
import gzip
import json
import magic
import requests
import pickle
import lmdb
import uuid
import subprocess
import argparse
import time
import S3
from concurrent.futures import ThreadPoolExecutor
import subprocess
import tarfile
from random import randint
from tqdm import tqdm
import logging
import logging.handlers

map_size = 100 * 1024 * 1024 * 1024 
logging.basicConfig(filename='harvester.log', filemode='w', level=logging.DEBUG)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''
Harvester for PDF available in open access. a LMDB index is used to keep track of the harvesting process and
possible failures.  

This version uses the standard ThreadPoolExecutor for parallelizing the download/processing/upload processes. 
Given the limits of ThreadPoolExecutor (input stored in memory, blocking Executor.map until the whole input
is processed and output stored in memory until all input is consumed), it works with batches of PDF of a size 
indicated in the config.json file (default is 100 entries). We are moving from first batch to the second one 
only when the first is entirely processed. 

'''
class OAHarverster(object):

    def __init__(self, config_path='./config.json', thumbnail=False, sample=None):
        self.config = None
        
        # standard lmdb environment for storing biblio entries by uuid
        self.env = None

        # lmdb environment for storing mapping between doi/pmcid and uuid
        self.env_doi = None

        # lmdb environment for keeping track of failures
        self.env_fail = None

        self._load_config(config_path)
        
        # boolean indicating if we want to generate thumbnails of front page of PDF 
        self.thumbnail = thumbnail
        self._init_lmdb()

        # if a sample value is provided, indicate that we only harvest the indicated number of PDF
        self.sample = sample

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
        # create the data path if it does not exist 
        if not os.path.isdir(self.config["data_path"]):
            try:  
                os.makedirs(self.config["data_path"])
            except OSError:  
                logging.exception("Creation of the directory %s failed" % self.config["data_path"])
            else:  
                logging.debug("Successfully created the directory %s" % self.config["data_path"])

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
        download in parallel PDF, generate thumbnails (if selected), upload resources locally 
        or on S3 and update the json description of the entries
        """
        batch_size_pdf = self.config['batch_size']
        # batch size for lmdb commit
        batch_size_lmdb = 10 
        n = 0
        i = 0
        urls = []
        entries = []
        filenames = []
        selection = None

        # check the overall number of entries based on the line number
        print("\ncalculating number of entries...")
        count = 0
        with gzip.open(filepath, 'rb') as gz:  
            while 1:
                buffer = gz.read(8192*1024)
                if not buffer: break
                count += buffer.count(b'\n')

        print("total entries: " + str(count))

        if self.sample is not None:
            # random selection corresponding to the requested sample size
            selection = [randint(0, count-1) for p in range(0, sample)]
            selection.sort()

        gz = gzip.open(filepath, 'rt')
        position = 0
        for line in tqdm(gz, total=count):
            if selection is not None and not position in selection:
                position += 1
                continue

            #if n >= 100:
            #    break
            if i == batch_size_pdf:
                self.processBatch(urls, filenames, entries)
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
                position += 1
                continue

            if 'best_oa_location' in entry:
                if entry['best_oa_location'] is not None:
                    if 'url_for_pdf' in entry['best_oa_location']:
                        pdf_url = entry['best_oa_location']['url_for_pdf']
                        if pdf_url is not None:    
                            #print(pdf_url)
                            urls.append(pdf_url)
                            # TBD: consider alternative non-best PDF URL for fallback solution?

                            entry['id'] = str(uuid.uuid4())
                            entries.append(entry)
                            filenames.append(os.path.join(self.config["data_path"], entry['id']+".pdf"))
                            i += 1
            position += 1
            
        gz.close()

        # we need to process the latest incomplete batch (if not empty)
        if len(urls) >0:
            self.processBatch(urls, filenames, entries)
            n += len(urls)

        print("total entries:", n)

    def harvestPMC(self, filepath):   
        """
        Main method for PMC, use the provided PMC list file for getting pdf url for Open Access resources, 
        or download the list file on NIH server if not provided, download in parallel PDF, generate thumbnails, 
        upload resources on S3 and update the json description of the entries
        """
        batch_size_pdf = self.config['batch_size']
        pmc_base = self.config['pmc_base']
        # batch size for lmdb commit
        batch_size_lmdb = 10 
        n = 0
        i = 0
        urls = []
        entries = []
        filenames = []

        selection = None

        # check the overall number of entries based on the line number
        print("calculating number of entries...")
        count = 0
        with open(filepath, 'rb') as fp:  
            while 1:
                buffer = fp.read(8192*1024)
                if not buffer: break
                count += buffer.count(b'\n')

        print("total entries: " + str(count))

        if self.sample is not None:
            # random selection corresponding to the requested sample size
            selection = [randint(0, count-1) for p in range(0, sample)]
            selection.sort()

        with open(filepath, 'rt') as fp:  
            position = 0
            for line in tqdm(gz, total=count):
                if selection is not None and not position in selection:
                    position += 1
                    continue

                # skip first line which gives the date when the list has been generated
                if position == 0:
                    position += 1
                    continue
                #if n >= 100:
                #    break
                if i == batch_size_pdf:
                    self.processBatch(urls, filenames, entries)#, txn, txn_doi, txn_fail)
                    # reinit
                    i = 0
                    urls = []
                    entries = []
                    filenames = []
                    n += batch_size_pdf

                # one PMC entry per line
                tokens = line.split('\t')
                subpath = tokens[0]
                pmcid = tokens[2]
                pmid = tokens[3]
                ind = pmid.find(":")
                if ind != -1:
                    pmid = pmid[ind+1:]
                
                if pmcid is None:
                    position += 1
                    continue

                # check if the entry has already been processed
                if self.getUUIDByDoi(pmcid) is not None:
                    position += 1
                    continue

                if subpath is not None:
                    entry = {}
                    tar_url = pmc_base + subpath
                    #print(tar_url)
                    urls.append(tar_url)

                    entry['id'] = str(uuid.uuid4())
                    entry['pmcid'] = pmcid
                    entry['pmid'] = pmid
                    # TODO: avoid depending on instanciated DOI
                    entry['doi'] = pmcid
                    entry_url = {}
                    entry_url['url_for_pdf'] = tar_url
                    entry['best_oa_location'] = entry_url
                    entries.append(entry)
                    filenames.append(os.path.join(self.config["data_path"], entry['id']+".tar.gz"))
                    i += 1

                position += 1
            
        # we need to process the latest incomplete batch (if not empty)
        if len(urls) >0:
            self.processBatch(urls, filenames, entries)#, txn, txn_doi, txn_fail)
            n += len(urls)

        print("total entries:", n)

    def processBatch(self, urls, filenames, entries):#, txn, txn_doi, txn_fail):
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(_download, urls, filenames, entries)

        # LMDB write transaction must be performed in the thread that created the transaction, so
        # we need to have the following lmdb updates out of the paralell process
        entries = []
        for result in results:
            local_entry = result[1]
            # conservative check if the downloaded file is of size 0 with a status code sucessful (code: 0),
            # it should not happen *in theory*
            # and check mime type
            valid_file = False
            local_filename = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
            if os.path.isfile(local_filename): 
                if _is_valid_file(local_filename, "pdf"):
                    valid_file = True
            
            local_filename = os.path.join(self.config["data_path"], local_entry['id']+".nxml")
            if os.path.isfile(local_filename): 
                if _is_valid_file(local_filename, "xml"):
                    valid_file = True

            if (result[0] is None or result[0] == "0" or result[0] == "success") and valid_file:
                #update DB
                with self.env.begin(write=True) as txn:
                    txn.put(local_entry['id'].encode(encoding='UTF-8'), _serialize_pickle(local_entry)) 

                with self.env_doi.begin(write=True) as txn_doi:
                    txn_doi.put(local_entry['doi'].encode(encoding='UTF-8'), local_entry['id'].encode(encoding='UTF-8'))

                entries.append(local_entry)
            else:
                logging.info("register harvesting failure: " + result[0])
                
                #update DB
                with self.env.begin(write=True) as txn:
                    txn.put(local_entry['id'].encode(encoding='UTF-8'), _serialize_pickle(local_entry))  

                with self.env_doi.begin(write=True) as txn_doi:
                    txn_doi.put(local_entry['doi'].encode(encoding='UTF-8'), local_entry['id'].encode(encoding='UTF-8'))

                with self.env_fail.begin(write=True) as txn_fail:
                    txn_fail.put(local_entry['id'].encode(encoding='UTF-8'), result[0].encode(encoding='UTF-8'))

                # if an empty pdf or tar file is present, we clean it
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".tar.gz")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".nxml")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)

        # finally we can parallelize the thumbnail/upload/file cleaning steps for this batch
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(self.manageFiles, entries)


    def processBatchReprocess(self, urls, filenames, entries):#, txn, txn_doi, txn_fail):
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(_download, urls, filenames, entries)
        
        # LMDB write transactions in the thread that created the transaction
        entries = []
        for result in results: 
            local_entry = result[1]
            if result[0] is None or result[0] == "0":
                entries.append(local_entry)
                # remove the entry in fail, as it is now sucessful
                with self.env_fail.begin(write=True) as txn_fail2:
                    txn_fail2.delete(local_entry['id'].encode(encoding='UTF-8'))
            else:
                # still an error
                # if an empty pdf file is present, we clean it
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".tar.gz")
                if os.path.isfile(local_filename): 
                    os.remove(local_filename)
                local_filename = os.path.join(self.config["data_path"], local_entry['id']+".nxml")
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
        local_filename_nxml = os.path.join(self.config["data_path"], local_entry['id']+".nxml")

        # generate thumbnails
        if self.thumbnail:
            generate_thumbnail(local_filename)
        
        dest_path = generateStoragePath(local_entry['id'])
        thumb_file_small = local_filename.replace('.pdf', '-thumb-small.png')
        thumb_file_medium = local_filename.replace('.pdf', '-thumb-medium.png')
        thumb_file_large = local_filename.replace('.pdf', '-thumb-large.png')

        if self.s3 is not None:
            # upload to S3 
            # upload is already in parallel for individual file (with parts)
            # so we don't further upload in parallel at the level of the files
            if os.path.isfile(local_filename):
                self.s3.upload_file_to_s3(local_filename, dest_path, storage_class='ONEZONE_IA')
            if os.path.isfile(local_filename_nxml):
                self.s3.upload_file_to_s3(local_filename_nxml, dest_path, storage_class='ONEZONE_IA')

            if (self.thumbnail):
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
                if os.path.isfile(local_filename):
                    shutil.copyfile(local_filename, os.path.join(local_dest_path, local_entry['id']+".pdf"))
                if os.path.isfile(local_filename_nxml):
                    shutil.copyfile(local_filename_nxml, os.path.join(local_dest_path, local_entry['id']+".nxml"))

                if (self.thumbnail):
                    if os.path.isfile(thumb_file_small):
                        shutil.copyfile(thumb_file_small, os.path.join(local_dest_path, local_entry['id']+"-thumb-small.png"))

                    if os.path.isfile(thumb_file_medium):
                        shutil.copyfile(thumb_file_medium, os.path.join(local_dest_path, local_entry['id']+"-thumb-medium.png"))

                    if os.path.isfile(thumb_file_large):
                        shutil.copyfile(thumb_file_large, os.path.join(local_dest_path, local_entry['id']+"-thumb-larger.png"))

            except IOError:
                logging.exception("invalid path")

        # clean pdf and thumbnail files
        try:
            if os.path.isfile(local_filename):
                os.remove(local_filename)
            if os.path.isfile(local_filename_nxml):
                os.remove(local_filename_nxml)
            if (self.thumbnail):
                if os.path.isfile(thumb_file_small): 
                    os.remove(thumb_file_small)
                if os.path.isfile(thumb_file_medium): 
                    os.remove(thumb_file_medium)
                if os.path.isfile(thumb_file_large): 
                    os.remove(thumb_file_large)
        except IOError:
            logging.exception("temporary file cleaning failed")   


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
        
        with self.env.begin(write=True) as txn:
            nb_total = txn.stat()['entries']

        with self.env_fail.begin(write=True) as txn_fail:
            nb_fails = txn_fail.stat()['entries']
        
        print("number of failed entries with OA link:", nb_fails, "out of", nb_total, "entries")

        # iterate over the fail lmdb
        with self.env.begin(write=True) as txn:
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
                #print(pdf_url)
                urls.append(pdf_url)
                entries.append(local_entry)
                if pdf_url.endswith(".tar.gz"):
                    filenames.append(os.path.join(self.config["data_path"], local_entry['id']+".tar.gz"))
                else:  
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
            # iterate over lmdb
            cursor = txn.cursor()
            for key, value in cursor:
                if txn.get(key) is None:
                    continue
                local_entry = _deserialize_pickle(txn.get(key))
                local_entry["id"] = key.decode(encoding='UTF-8');
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

        # clean any possibly remaining tmp files (.pdf and .png)
        for f in os.listdir(self.config["data_path"]):
            if f.endswith(".pdf") or f.endswith(".png") or f.endswith(".nxml") or f.endswith(".tar.gz") or f.endswith(".xml"):
                os.remove(os.path.join(self.config["data_path"], f))
            # clean any existing data files, except 
            path = os.path.join(self.config["data_path"], f)
            if os.path.isdir(path):
                try:
                    shutil.rmtree(path)
                except OSError:
                    logging.exception("Error cleaning tmp files")

        # re-init the environments
        self._init_lmdb()

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


def _download(url, filename, entry):
    result = _download_requests(url, filename)
    if result != "success":
        result = _download_wget(url, filename)

    if os.path.isfile(filename) and filename.endswith(".tar.gz"):
        _manage_pmc_archives(filename)

    return result, entry

def _download_wget(url, filename):
    """ 
    First try with Python requests (which handle well compression), then move to a more robust download approach
    """
    result = "fail"
    # This is the most robust and reliable way to download files I found with Python... to rely on system wget :)
    #cmd = "wget -c --quiet" + " -O " + filename + ' --connect-timeout=10 --waitretry=10 ' + \
    cmd = "wget -c --quiet" + " -O " + filename + ' --timeout=15 --waitretry=0 --tries=5 --retry-connrefused ' + \
        '--header="User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0" ' + \
        '--header="Accept: application/pdf, text/html;q=0.9,*/*;q=0.8" --header="Accept-Encoding: gzip, deflate" ' + \
        '--no-check-certificate ' + \
        '"' + url + '"'
    try:
        result = subprocess.check_call(cmd, shell=True)
        
        # if the used version of wget does not decompress automatically, the following ensures it is done
        result_compression = _check_compression(filename)
        if not result_compression:
            # decompression failed, or file is invalid
            if os.path.isfile(filename):
                try:
                    os.remove(filename)
                except OSError:
                    logging.exception("Deletion of invalid compressed file failed")
                    result = "fail"
            # ensure cleaning
            if os.path.isfile(filename+'.decompressed'):
                try:
                    os.remove(filename+'.decompressed')
                except OSError:  
                    logging.exception("Final deletion of temp decompressed file failed")
        else:
            result = "success"

    except subprocess.CalledProcessError as e:  
        logging.exception("error subprocess wget") 
        #logging.error("wget command was: " + cmd)
        result = "fail"

    except Exception as e:
        logging.exception("Unexpected error wget process") 
        result = "fail"

    return str(result)

def _download_requests(url, filename):
    """ 
    Download with Python requests which handle well compression, but not very robust and bad parallelization
    """
    HEADERS = {"""User-Agent""": """Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0"""}
    result = "fail" 
    try:
        file_data = requests.get(url, allow_redirects=True, headers=HEADERS, verify=False, timeout=20)
        if file_data.status_code == 200:
            with open(filename, 'wb') as f_out:
                f_out.write(file_data.content)
            result = "success"
    except Exception:
        logging.exception("Download failed for {0} with requests".format(url))
    return result

def _check_compression(file):
    '''
    check if a file is compressed, if yes decompress and replace by the decompressed version
    '''
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
        if file_type == 'application/gzip':
            success = False
            # decompressed in tmp file
            with gzip.open(file, 'rb') as f_in:
                with open(file+'.decompressed', 'wb') as f_out:
                    try:
                        shutil.copyfileobj(f_in, f_out)
                    except OSError:  
                        logging.exception("Decompression file failed")
                    else:
                        success = True
            # replace the file
            if success:
                try:
                    shutil.copyfile(file+'.decompressed', file)
                except OSError:  
                    logging.exception("Replacement of decompressed file failed")
                    success = False
            # delete the tmp file
            if os.path.isfile(file+'.decompressed'):
                try:
                    os.remove(file+'.decompressed')
                except OSError:  
                    logging.exception("Deletion of temp decompressed file failed")    
            return success
        else:
            return True
    return False

def _is_valid_file(file, mime_type):
    target_mime = []
    if mime_type == 'xml':
        target_mime.append("application/xml")
        target_mime.append("text/xml")
    elif mime_type == 'png':
        target_mime.append("image/png")
    else:
        target_mime.append("application/"+mime_type)
    file_type = ""
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
    return file_type in target_mime

def _manage_pmc_archives(filename):
    # check if finename exists and we have downloaded an archive rather than a PDF (case ftp PMC)
    if os.path.isfile(filename) and filename.endswith(".tar.gz"):
        try:
            # for PMC we still have to extract the PDF from archive
            #print(filename, "is an archive")
            thedir = os.path.dirname(filename)
            # we need to extract the PDF, the NLM extra file, change file name and remove the tar file
            tar = tarfile.open(filename)
            pdf_found = False
            # this is a unique temporary subdirectory to extract the relevant files in the archive, unique directory is
            # introduced to avoid several files with the same name from different archives to be extracted in the 
            # same place 
            basename = os.path.basename(filename)
            tmp_subdir = basename[0:6]
            for member in tar.getmembers():
                if not pdf_found and member.isfile() and (member.name.endswith(".pdf") or member.name.endswith(".PDF")):
                    member.name = os.path.basename(member.name)
                    # create unique subdirectory
                    if not os.path.exists(os.path.join(thedir,tmp_subdir)):
                        os.mkdir(os.path.join(thedir,tmp_subdir))
                    f = tar.extract(member, path=os.path.join(thedir,tmp_subdir))
                    #print("extracted file:", member.name)
                    # be sure that the file exists (corrupted archives are not a legend)
                    if os.path.isfile(os.path.join(thedir,tmp_subdir,member.name)):
                        os.rename(os.path.join(thedir,tmp_subdir,member.name), filename.replace(".tar.gz", ".pdf"))                        
                        pdf_found = True
                    # delete temporary unique subdirectory
                    try:
                        shutil.rmtree(os.path.join(thedir,tmp_subdir))
                    except OSError:  
                        logging.exception("Deletion of tmp dir failed: " + os.path.join(thedir,tmp_subdir))     
                    #break
                if member.isfile() and member.name.endswith(".nxml"):
                    member.name = os.path.basename(member.name)
                    # create unique subdirectory
                    if not os.path.exists(os.path.join(thedir,tmp_subdir)):
                        os.mkdir(os.path.join(thedir,tmp_subdir))
                    f = tar.extract(member, path=os.path.join(thedir,tmp_subdir))
                    #print("extracted file:", member.name)
                    # be sure that the file exists (corrupted archives are not a legend)
                    if os.path.isfile(os.path.join(thedir,tmp_subdir,member.name)):
                        os.rename(os.path.join(thedir,tmp_subdir,member.name), filename.replace(".tar.gz", ".nxml"))
                    # delete temporary unique subdirectory
                    try:
                        shutil.rmtree(os.path.join(thedir,tmp_subdir))
                    except OSError:  
                        logging.exception("Deletion of tmp dir failed: " + os.path.join(thedir,tmp_subdir))      
            tar.close()
            if not pdf_found:
                logging.warning("no pdf found in archive: " + filename)
            if os.path.isfile(filename):
                try:
                    os.remove(filename)
                except OSError:  
                    logging.exception("Deletion of PMC archive file failed: " + filename) 
        except Exception as e:
            logging.exception("Unexpected error")
            pass


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
        logging.exception("error thumb-small.png")

    thumb_file = pdfFile.replace('.pdf', '-thumb-medium.png')
    cmd = 'convert -quiet -density 200 -thumbnail x300 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        logging.exception("error thumb-small.png")

    thumb_file = pdfFile.replace('.pdf', '-thumb-large.png')
    cmd = 'convert -quiet -density 200 -thumbnail x500 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        logging.exception("error thumb-small.png")

def generateStoragePath(identifier):
    '''
    Convert a file name into a path with file prefix as directory paths:
    123456789 -> 12/34/56/123456789
    '''
    return os.path.join(identifier[:2], identifier[2:4], identifier[4:6], identifier[6:8], "")

def test():
    harvester = OAHarverster()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Open Access PDF harvester")
    parser.add_argument("--unpaywall", default=None, help="path to the Unpaywall dataset (gzipped)") 
    parser.add_argument("--pmc", default=None, help="path to the pmc file list, as available on NIH's site") 
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    parser.add_argument("--dump", default="dump.json", help="write all JSON entries having a sucessful OA link with their UUID") 
    parser.add_argument("--reprocess", action="store_true", help="reprocessed failed entries with OA link") 
    parser.add_argument("--reset", action="store_true", help="ignore previous processing states, and re-init the harvesting process from the beginning") 
    parser.add_argument("--increment", action="store_true", help="augment an existing harvesting with a new released Unpaywall dataset (gzipped)") 
    parser.add_argument("--thumbnail", action="store_true", help="generate thumbnail files for the front page of the PDF") 
    parser.add_argument("--sample", type=int, default=None, help="Harvest only a random sample of indicated size")

    args = parser.parse_args()

    unpaywall = args.unpaywall
    pmc = args.pmc
    config_path = args.config
    reprocess = args.reprocess
    reset = args.reset
    dump = args.dump
    thumbnail = args.thumbnail
    sample = args.sample

    harvester = OAHarverster(config_path=config_path, thumbnail=thumbnail, sample=sample)

    if reset:
        harvester.reset()

    start_time = time.time()

    if reprocess:
        harvester.reprocessFailed()
        harvester.diagnostic()
    elif unpaywall is not None: 
        harvester.harvestUnpaywall(unpaywall)
        harvester.diagnostic()
    elif pmc is not None: 
        harvester.harvestPMC(pmc)
        harvester.diagnostic()

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))

    if dump is not None:
        harvester.dump(dump)
