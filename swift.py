import os

# support for SWIFT object storage
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject

# logging
import logging
import logging.handlers
logging.basicConfig(filename='harvester.log', filemode='w', level=logging.DEBUG)

class Swift(object):
    
    def __init__(self, config):
        self.config = config

        options = self._init_swift_options()
        options['object_uu_threads'] = 20
        #print(options)
        self.swift = SwiftService(options=options)
        container_names = []

        try:
            list_account_part = self.swift.list()
            for page in list_account_part:
                print(page)
                if page["success"]:
                    for item in page["listing"]:
                        i_name = item["name"]
                        container_names.append(i_name)
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
        for key in self.config["swift"]:
            if len(self.config["swift"][key].strip())>0:
                options[key] = self.config["swift"][key]
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

        # in the documentation they call that "dir marker", which seems to be loading an empty directory path object
        # very likely useless for us
        #obj = SwiftUploadObject(None, object_name=dest_path, options={'dir_marker': True})
        #objs.append(obj)

        try:
            result = swift.upload(self.config["swift_container"], objs)
            if not r['success']:
                error = r['error']
                if r['action'] == "upload_object":
                    logging.error("Failed to upload object %s to container %s: %s" % (container, r['object'], error))
                else:
                    logging.error("%s" % error)

        except SwiftError:
            logging.exception("error uploading file to SWIFT container")

    def download_file(self, file_path, dest_path):
        """
        Download a file given a path and returns the download destination file path.
        """


    def get_swift_list(self, dir_name):
        """
        Return all contents of a given dir in SWIFT object storage.
        Goes through the pagination to obtain all file names.
        """

    def remove_file(self, file_path):
        """
        Remove an existing file on the SWIFT object storage
        """

