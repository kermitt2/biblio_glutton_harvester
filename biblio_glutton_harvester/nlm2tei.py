import argparse
import os
import shutil
import subprocess
import biblio_glutton_harvester.S3 as S3
import biblio_glutton_harvester.swift as swift
import json
import time
from biblio_glutton_harvester.OAHarvester import generateStoragePath, _load_config

# logging
import logging
import logging.handlers

logging.basicConfig(filename='harvester.log', filemode='w', level=logging.DEBUG)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging.getLogger("keystoneclient").setLevel(logging.ERROR)
logging.getLogger("swiftclient").setLevel(logging.ERROR)

class Nlm2tei(object):
    """
    Convert existing NLM/JATS files (PMC) in a data repository into TEI XML format similar as Grobid output.
    This is using Pub2TEI (https://github.com/kermitt2/Pub2TEI) and it is done in batch to have good runtime. 

    Note: this requires a JRE 8 or more
    """
    def __init__(self, config_path='./config.yaml'):
        self.config = _load_config(config_path)

        # check Pub2TEI directory indicated on the config file
        if not os.path.isdir(self.config["pub2tei_path"]):
            print("Error: path to Pub2TEI is not valid, please git clone https://github.com/kermitt2/Pub2TEI", 
                  "and indicate the path to the cloned directory in the config file)")

        self.s3 = None
        if "aws" in self.config and "bucket_name" in self.config["aws"] and self.config["aws"]["bucket_name"] and len(self.config["aws"]["bucket_name"].strip()) > 0:
            self.s3 = S3.S3(self.config["aws"])

        self.swift = None
        if "swift" in self.config and self.config["swift"] and len(self.config["swift"])>0 and "swift_container" in self.config["swift"] and self.config["swift"]["swift_container"] and len(self.config["swift"]["swift_container"])>0:
            self.swift = swift.Swift(self.config["swift"], data_path=self.config["data_path"])

    def _create_batch_input(self, force=False):
        """
        Walk through the data directory, grab all the .nxml files and put them in a single temporary working directory
        """
        temp_dir = os.path.join(self.config["data_path"], "pub2tei_tmp")
        # remove tmp dir if already exists
        if os.path.isdir(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

        # create the tmp dir
        try:  
            os.makedirs(temp_dir)
        except OSError:  
            print ("Creation of the directory %s failed" % temp_dir)
        else:  
            print ("Successfully created the directory %s" % temp_dir)

        # walk through the data directory, copy .nxml files to the temp directory
        for root, dirs, files in os.walk(self.config["data_path"]):
            for the_file in files:
                # normally all NLM/JATS files are stored with extension .nxml, but for safety we also cover .nlm extension
                if the_file.endswith(".nxml") or the_file.endswith(".nlm") or the_file.endswith(".nxml.xml"):
                    #print(root, the_file)
                    # check if the TEI file has already been generated, except if we force the re-process
                    identifier = the_file.split(".")[0]
                    if not force and os.path.isfile(os.path.join(root,identifier+".pub2tei.tei.xml")):
                        continue
                    if not os.path.isfile(os.path.join(temp_dir,the_file)):
                        shutil.copy(os.path.join(root,the_file), temp_dir)

        # add dummy DTD files for JATS to avoid errors and crazy online DTD download
        open(os.path.join(temp_dir,"JATS-archivearticle1.dtd"), 'a').close()
        open(os.path.join(temp_dir,"JATS-archivearticle1-mathml3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"JATS-archivearticle1-3-mathml3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle1-mathml3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle1.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"journalpublishing.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle.dtd"), 'a').close()
        return temp_dir

    def process_batch(self, dir_path):
        """
        Apply Pub2TEI to all the files of indicated directory
        """
        temp_dir_out = os.path.join(dir_path, "out")
        try:  
            os.makedirs(temp_dir_out)
        except OSError:  
            print ("Creation of the directory %s failed" % temp_dir_out)

        cmd = "java -jar " + os.path.join(self.config["pub2tei_path"],"Samples","saxon9he.jar") + " -s:" + dir_path + \
            " -xsl:" + os.path.join(self.config["pub2tei_path"],"Stylesheets","Publishers.xsl") + \
            " -o:" + temp_dir_out + " -dtd:off -a:off -expand:off -t " + \
            " --parserFeature?uri=http%3A//apache.org/xml/features/nonvalidating/load-external-dtd:false"
        #print(cmd)
        try:
            result = subprocess.check_call(cmd, shell=True)
        except subprocess.CalledProcessError as e:   
            print("e.returncode", e.returncode)
            print("e.output", e.output)
            #if e.output is not None and e.output.startswith('error: {'):
            if  e.output is not None:
                error = json.loads(e.output[7:]) # Skip "error: "
                print("error code:", error['code'])
                print("error message:", error['message'])
                result = error['message']
            else:
                result = e.returncode
        return str(result)

    def _manage_batch_results(self, temp_dir):
        """
        Copy results from the temporary working directory to the data directory, clean temp stuff
        """
        if not os.path.isdir(temp_dir):
            print("provided directory is not valid:", temp_dir)
            return

        temp_dir_out = os.path.join(temp_dir, "out")
        if not os.path.isdir(temp_dir_out):
            print("result temp dir is not valid:", temp_dir_out)
            return

        for f in os.listdir(temp_dir_out):
            if f.endswith(".nxml.xml") or f.endswith(".nxml") or f.endswith(".nlm") or f.endswith(".nlm.xml"):
                # move the file back to its storage location (which can be S3)
                identifier = f.split(".")[0]
                if self.s3 is not None:
                    # upload results on S3 bucket
                    self.s3.upload_file_to_s3(identifier+".pub2tei.tei.xml", os.path.join(generateStoragePath(identifier), identifier), storage_class='ONEZONE_IA')
                elif self.swift is not None:
                    # upload results on swift object storage
                    self.swift.upload_file_to_swift(identifier+".pub2tei.tei.xml", os.path.join(generateStoragePath(identifier), identifier))
                else:   
                    dest_path = os.path.join(self.config["data_path"], generateStoragePath(identifier), identifier, identifier+".pub2tei.tei.xml")
                    shutil.copyfile(os.path.join(temp_dir_out,f), dest_path)
        
        # clean temp dir
        try:
            shutil.rmtree(temp_dir)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
        
    def process(self, force=False):
        """
        Launch the conversion process
        """
        start_time = time.time()
        temp_dir = self._create_batch_input(force=force)    
        self.process_batch(temp_dir)
        self._manage_batch_results(temp_dir)  
        # TBD: consolidate raw reference string present in the converted TEI
        runtime = round(time.time() - start_time, 3)
        print("\nruntime: %s seconds " % (runtime))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Converter NLM to TEI")
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

    nlm2tei = Nlm2tei(config_path=config_path)
    nlm2tei.process(force=force)

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))

    exit(0)