import os
import shutil
import subprocess

# support for SWIFT object storage
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject

from biblio_glutton_harvester.OAHarvester import _check_compression

# logging
import logging
import logging.handlers
logging.basicConfig(filename='harvester.log', filemode='w', level=logging.DEBUG)

class Swift(object):
    
    def __init__(self, config, data_path="./data/"):
        self.config = config
        self.data_path = data_path

        options = self._init_swift_options()
        options['object_uu_threads'] = 20
        self.swift = SwiftService(options=options)
        container_names = []
        try:
            list_account_part = self.swift.list()
            for page in list_account_part:
                if page["success"]:
                    for item in page["listing"]:
                        i_name = item["name"]
                        container_names.append(i_name)
                        #if i_name == self.config["swift_container"]:
                        #    print("using SWIFT", self.config["swift_container"], "container:", item)
                else:
                    logging.error("error listing SWIFT object storage containers")

        except SwiftError as e:
            logging.exception("error listing containers")

        if self.config["swift_container"] not in container_names: 
            # create the container
            try:
                self.swift.post(container=self.config["swift_container"])
            except SwiftError:
                logging.exception("error creating SWIFT object storage container " + self.config["swift_container"])
        else:
            logging.debug("container already exists on SWIFT object storage: " + self.config["swift_container"])

    def _init_swift_options(self):
        options = {}
        if "swift_parameters" in self.config:
            for key in self.config["swift_parameters"]:
                if len(self.config["swift_parameters"][key].strip())>0:
                    options[key] = self.config["swift_parameters"][key]
        else:
            for key in self.config:
                if len(self.config[key].strip())>0:
                    options[key] = self.config[key]
        return options

    def upload_file_to_swift(self, file_path, dest_path=None):
        """
        Upload the given file to current SWIFT object storage container
        """
        objs = []

        # file object
        file_name = os.path.basename(file_path)
        object_name = file_name
        if dest_path != None:
            object_name = dest_path + "/" + file_name
            
        obj = SwiftUploadObject(file_path, object_name=object_name)
        objs.append(obj)
        try:
            for result in self.swift.upload(self.config["swift_container"], objs):
                if not result['success']:
                    error = result['error']
                    if result['action'] == "upload_object":
                        logging.error("Failed to upload object %s to container %s: %s" % (self.config["swift_container"], result['object'], error))
                    else:
                        logging.error("%s" % error)
        except SwiftError:
            logging.exception("error uploading file to SWIFT container")

    def upload_files_to_swift(self, file_paths, dest_path=None):
        """
        Bulk upload of a list of files to current SWIFT object storage container under the same destination path
        """
        objs = []

        # file object
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            object_name = file_name
            if dest_path != None:
                object_name = dest_path + "/" + file_name
                
            obj = SwiftUploadObject(file_path, object_name=object_name)
            objs.append(obj)

        try:
            for result in self.swift.upload(self.config["swift_container"], objs):
                if not result['success']:
                    error = result['error']
                    if result['action'] == "upload_object":
                        logging.error("Failed to upload object %s to container %s: %s" % (self.config["swift_container"], result['object'], error))
                    else:
                        logging.error("%s" % error)
        except SwiftError:
            logging.exception("error uploading file to SWIFT container")

    def download_file(self, file_path, dest_path):
        """
        Download a file given a path and returns the download destination file path.
        """
        #print("download_file:", file_path, dest_path)

        prefix = os.path.dirname(file_path)
        outfile = os.path.basename(dest_path)
        opts = {
            'yes_all': False,
            'marker': '',
            'prefix': prefix,
            'no_download': False,
            'header': [],
            'skip_identical': False,
            'out_directory': self.data_path,
            'checksum': True,
            'out_file': None,
            'remove_prefix': True,
            'shuffle' : False
        }

        objs = [ file_path ]
        try:
            for down_res in self.swift.download(container=self.config["swift_container"], objects=objs, options=opts):
                #print(down_res)
                if down_res['success']:
                    #print("'%s' downloaded" % down_res['object'])
                    local_path = down_res['path']

                    # decompress if required
                    if local_path.endswith(".gz"):
                        try:
                            subprocess.check_call(['gunzip', '-f', local_path])
                            local_path = local_path.replace(".gz", "")
                        except:
                            logging.error("gunzip failed")

                    if local_path != dest_path:
                        shutil.move(local_path, dest_path)
                else:
                    logging.error("'%s' download failed" % down_res['object'])
                    return None
        except SwiftError:
            logging.exception("error downloading file from SWIFT container")
            return None

        return dest_path

    def get_swift_list(self, dir_name=None):
        """
        Return all contents of a given dir in SWIFT object storage.
        Goes through the pagination to obtain all file names.

        afaik, this is terribly inefficient, as we have to go through all the objects of the storage.
        """
        result = []
        try:
            list_parts_gen = self.swift.list(container=self.config["swift_container"])
            for page in list_parts_gen:
                if page["success"]:
                    for item in page["listing"]:
                        if dir_name == None or item["name"].startswith(dir_name):
                            result.append(item["name"])    
                else:
                    logging.error(page["error"])
        except SwiftError as e:
            logger.error(e.value)
        return result

    def remove_file(self, file_path):
        """
        Remove an existing file on the SWIFT object storage
        """ 
        try:
            objs = [ file_path ]
            for result in self.swift.delete(self.config["swift_container"], objs):
                if not result['success']:
                    error = result['error']
                    if result['action'] == "delete_object":
                        logging.error("Failed to delete object %s from container %s: %s" % (self.config["swift_container"], result['object'], error))
                    else:
                        logging.error("%s" % error)
        except SwiftError:
            logging.exception("error removing file from SWIFT container")

    def remove_all_files(self):
        """
        Remove all the existing files on the SWIFT object storage
        """
        try:
            list_parts_gen = self.swift.list(container=self.config["swift_container"])
            for page in list_parts_gen:
                if page["success"]:
                    to_delete = []
                    for item in page["listing"]:
                        to_delete.append(item["name"])
                    for del_res in self.swift.delete(container=self.config["swift_container"], objects=to_delete):
                        if not del_res['success']:
                            error = del_res['error']
                            if del_res['action'] == "delete_object":
                                logging.error("Failed to delete object %s from container %s: %s" % (self.config["swift_container"], del_res['object'], error))
                            else:
                                logging.error("%s" % error)
        except SwiftError:
            logging.exception("error removing all files from SWIFT container")
