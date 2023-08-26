import argparse
import os
import shutil
import subprocess
import biblio_glutton_harvester.S3 as S3
import biblio_glutton_harvester.swift as swift
import json
import time
from biblio_glutton_harvester.OAHarvester import generateStoragePath, _load_config, _serialize_pickle, _deserialize_pickle
import zipfile
import lmdb
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# logging
import logging
import logging.handlers

logging.basicConfig(filename='harvester.log', filemode='w', level=logging.DEBUG)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging.getLogger("keystoneclient").setLevel(logging.ERROR)
logging.getLogger("swiftclient").setLevel(logging.ERROR)

class LaTeX2tei(object):
    """
    Convert existing LaTeX source files (zipped) in a data repository into TEI XML format similar as Grobid output.
    This is using a forked LaTeXML (https://github.com/kermitt2/LaTeXML), which needs to be installed preliminary. 

    In case of multiple .tex files, identifying the root latex file is necessary for arXiv sources. It is done by 
    inspecting the .tex files and looking for the main header and declaration.
    """
    def __init__(self, config_path='./config.yaml'):
        self.config = _load_config(config_path)

        # standard lmdb environment for storing biblio entries by uuid
        #self.env = None

        # check LaTeXML directory indicated on the config file
        if not os.path.isdir(self.config["latexml_path"]):
            print("Error: path to LaTeXML is not valid, please git clone https://github.com/kermitt2/LaTeXML", 
                  "and indicate the path to the cloned directory in the config file)")

        self.s3 = None
        if "aws" in self.config and "bucket_name" in self.config["aws"] and self.config["aws"]["bucket_name"] and len(self.config["aws"]["bucket_name"].strip()) > 0:
            self.s3 = S3.S3(self.config["aws"])

        self.swift = None
        if "swift" in self.config and self.config["swift"] and len(self.config["swift"])>0 and "swift_container" in self.config["swift"] and self.config["swift"]["swift_container"] and len(self.config["swift"]["swift_container"])>0:
            self.swift = swift.Swift(self.config["swift"], data_path=self.config["data_path"])


    def process(self, force=False):
        """
        Walk through the data directory, extract/examine all the .zip files to identify root latex source,
        apply the LaTeXML transformation to TEI XML. Rename the zip file to explicitely indicate that
        we have latex sources in it. 
        """
        batch_size = 100

        # for batch processing, store the latex zip archive file and zip parent directories 
        the_files = []
        the_roots = []

        # walk through the data directory, copy .nxml files to the temp directory
        for root, dirs, files in os.walk(self.config["data_path"]):
            for the_file in files:
                if the_file.endswith(".zip"):
                    # check if the TEI file has not already been generated for this source, except if the re-processing is forced
                    # check if the TEI file has already been generated, except if we force the re-process
                    identifier = the_file.split(".")[0]
                    if not force and os.path.isfile(os.path.join(root,identifier+".latex.tei.xml")):
                        continue

                    if len(the_files) >= batch_size:
                        self.process_batch(the_files, the_roots)

                    the_files.append(the_file)
                    the_roots.append(root)

        # process last batch if not empty
        if len(the_files) >= 0:
            self.process_batch(the_files, the_roots)  

    def process_batch(self, the_files, the_roots):
        with ProcessPoolExecutor(max_workers=8) as executor:
            results = executor.map(self.process_archive_file, the_files, the_roots)#, timeout=60)

    def process_archive_file(self, zip_file, root):
        '''
        Examine a latex zip source file and process the main latex file
        '''
        # unzip this zip thing, this is the easiest way to manage then latex files
        directory_to_extract_to = os.path.join(self.config["data_path"], zip_file.replace(".zip", "_zip_tmp"))
        try:
            os.mkdir(directory_to_extract_to)
            with zipfile.ZipFile(os.path.join(root,zip_file), 'r') as zip_ref:
                zip_ref.extractall(directory_to_extract_to)
            # examine tex files and find root latex file
            latex_files = []
            for root2, dirs2, files2 in os.walk(directory_to_extract_to):
                for the_file2 in files2:
                    if the_file2.endswith(".tex"):
                        latex_files.append(os.path.join(root2,the_file2))

            status_result = "fail"
            if zip_file.endswith(".latex.zip"):
                latex_tei_file = os.path.join(root, zip_file.replace(".zip", ".tei.xml"))
            else:
                latex_tei_file = os.path.join(root, zip_file.replace(".zip", ".latex.tei.xml"))
            if len(latex_files) == 1:
                # easy case, just one latex file 
                status_result = self.latexml2tei(latex_files[0], directory_to_extract_to, latex_tei_file)
                #print(latex_files[0], status_result)
            else:
                # examine the latex files to find the root
                root_file = _find_root_latex(latex_files)
                if root_file != None:
                    status_result = self.latexml2tei(root_file, directory_to_extract_to, latex_tei_file)
                    #print(root_file, status_result)
            if status_result == "success":
                # update catalog following outcome of the conversion
                local_entry = None
                update_entry = False
        except:
            logging.exception("Failed to process the archive file: " + os.path.join(root,zip_file))
        finally:
            # delete extraction directory
            shutil.rmtree(directory_to_extract_to)

            # rename zip file if latex sources are indeed there
            if not zip_file.endswith(".latex.zip"):
                os.rename(os.path.join(root,zip_file), os.path.join(root,zip_file.replace(".zip", ".latex.zip")))

    def latexml2tei(self, root_latex_file, directory_to_extract_to, tei_destination):
        #print("LaTeXML2tei", root_latex_file, directory_to_extract_to, tei_destination)

        result = "fail"
        latexml_file = os.path.join(directory_to_extract_to, root_latex_file.replace(".tex", ".xml"))

        # processing takes two steps, first produce the LaTeXML XML, then transforming this XML into TEI
        cmd1 = os.path.join(self.config["latexml_path"], "./blib/script/latexml")
        cmd1 += " --destination " + latexml_file + " --quiet --path " + directory_to_extract_to + " " + root_latex_file

        cmd2 = os.path.join(self.config["latexml_path"], "./blib/script/latexmlpost")
        cmd2 += " --destination " + tei_destination + " --quiet --pmml --mathtex --format tei  --novalidate --nopictureimages --nographicimages --sourcedirectory " + directory_to_extract_to + " " + latexml_file

        try:
            #result = subprocess.check_call(cmd1, shell=True)
            result = subprocess.run(cmd1, shell=True, check=True, timeout=240)
            #result = subprocess.check_call(cmd2, shell=True)
            result = subprocess.run(cmd2, shell=True, check=True, timeout=60)
            result = "success"
        except subprocess.CalledProcessError as e: 
            print("e.returncode", e.returncode)
            #print("e.output", e.output)
            #if e.output is not None and e.output.startswith('error: {'):
            if e.output is not None:
                error = json.loads(e.output[7:]) # Skip "error: "
                print("error code:", error['code'])
                print("error message:", error['message'])
                result = error['message']
            else:
                result = e.returncode
        finally:
            # clear the latexml scories
            for the_file in os.listdir("."):
                if the_file.endswith(".log") and the_file != "harvested.log":
                    os.remove(the_file)

        return result

    def post_process(self):
        # for some unknown reason, there are still some tmp repository not cleaned
        # clean any possibly remaining tmp files 
        for file_path in os.listdir(self.config["data_path"]):
            # clean any existing data files  
            if os.path.isdir(file_path) and file_path.endswith("_zip_tmp"):
                try:
                    shutil.rmtree(file_path)
                except OSError:
                    logging.exception("Error cleaning tmp files: " + file_path)

        # walk through the data directory for cleaning remaining numerous latexml generated files
        for root, dirs, files in os.walk(self.config["data_path"]):
            for the_file in files:
                if the_file.endswith(".css") or the_file.endswith(".cache"):
                    os.remove(os.path.join(root,the_file))
                elif the_file.endswith(".png") and the_file.startswith("x") and the_file.find("-thumb-") == -1:
                    os.remove(os.path.join(root,the_file))

def _find_root_latex(latex_file_lists):
    for latex_file in latex_file_lists:
        with open(latex_file) as file:
            for line in file:
                if line.startswith("\\document"):
                    # it should be enough for \documentclass and \documentstyle
                    return latex_file
    return latex_file_lists[0]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Converter LaTeX document to TEI")
    parser.add_argument("--config", default="./config.yaml", help="path to the config file, default is ./config.yaml") 
    parser.add_argument(
        "--force",
        action="store_true",
        help="force re-processing input files when TEI output files already exist",
    )
    args = parser.parse_args()
    config_path = args.config
    force = args.force
    
    start_time = time.time()

    latex2tei = LaTeX2tei(config_path=config_path)
    latex2tei.process(force=force)

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))

    # cleaning
    latex2tei.post_process()
    exit(0)
