# Open Access PDF harvester

Python utility for harvesting efficiently a large Open Access collection of PDF: 

* Uploaded PDF can be stored either on an Amazon S3 bucket or in a local storage. 

* Downloads and uploads over HTTP are multi-threaded for best robustness and efficiency. 

* Download supports redirections, https protocol and uses robust request headers. 

* The harvesting process can be interrupted and resumed.

* The tool is fault tolerant, it will keep track of the failed resource access with corresponding errors and makes possible subsequent retry on this subset. 

* As a bonus, image thumbnails of the front page of the PDF are created and stored with the PDF.

## Requirements

The utility has been tested with Python 3.5. It is developed for a deployment on a POSIX/Linux server (it uses imagemagick as external process to generate thumbnails and wget). An S3 account and bucket must have been created for non-local storage of the data collection. 

## Install

Get the github repo:

> git clone https://github.com/kermitt2/biblio-glutton-harvester

> cd biblio-glutton-harvester

It is advised to setup first a virtual environment to avoid falling into one of these gloomy python dependency marshlands:

> virtualenv --system-site-packages -p python3 env

> source env/bin/activate

Install the dependencies, use:

> pip3 install -r requirements.txt

For generating thumbnails corresponding to the harvested PDF, ImageMagick must be installed. For instance on Ubuntu:

> apt-get install imagemagick

A configuration file must be completed, by default the file `config.json` will be used, but it is also possible to use it as a template and specifies a particular configuration file when using the tool. In the configuration file, the information related to the S3 bucket to be used for uploading the resources must be filed, otherwise the resources will be stored locally in the indicated `data_path`. `nb_threads` gives the number of threads that can be used for parallel downloading and processing of the files. 

```json
{
    "data_path": "./data",
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "bucket_name": "",
    "nb_threads": 10
}
```

## Usage and options

### Harvesting of Unpaywall dataset


```
usage: OAHarvester.py [-h] [--unpaywall UNPAYWALL] [--config CONFIG]
                      [--reprocess]

OA PDF harvester

optional arguments:
  -h, --help            show this help message and exit
  --unpaywall UNPAYWALL
                        path to the Unpaywall dataset (gzipped)
  --config CONFIG       path to the config file, default is ./config.json
  --dump DUMP           Write all JSON entries having a sucessful OA link with
                        their UUID
  --reprocess           Reprocessed failed entries with OA link
  --reset               Ignore previous processing states, and re-init the
                        harvesting process from the beginning  
```

For processing all entries of an Unpaywall snapshot:

```bash
> python3 OAHarvester.py --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

If the process is interrupted, relaunching the above command will resume the process at the interruption point. For re-starting the process from the beginning, and removing existing local information about the state of process, use the parameter `--reset`:

```bash
> python3 OAHarvester.py --reset --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

After the completion of the snapshot, we can retry the PDF harvesting for the failed entries with the parameter `--reprocess`:

```bash
> python3 OAHarvester.py --reprocess --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

Entries having a sucessful OA PDF link can be dumped in JSON with the following command:

```bash
> python3 OAHarvester.py --dump output.json
```

Each entry having a successful OA link is present in the dump with the original JSON information as in the Unpaywall dataset, plus an UUID given by the attribute `id`.

```json
{ 
    "doi_url": "https://doi.org/10.4097/kjae.1988.21.5.833",
    "id": "1ba0cce3-335b-46d8-b29f-9cdfb6430fd2" 
    ...
}
```

The UUID can then be used for accessing the resources for this entry, the prefix path being based on the first 8 characters of the UUID, as follow: 

- PDF: `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.pdf`

- thumbnail small (150px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-small.png`

- thumbnail small (300px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-medium.png`

- thumbnail small (500px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-large.png`

Depending on the config, the resources can be accessed either locally under `data_path` or on AWS S3 following the URL prefix: `https://bucket_name.s3.amazonaws.com/`, for instance `https://bucket_name.s3.amazonaws.com/1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.pdf` - if you have the appropriate access rights.

## License and contact

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0). The dependencies used in the project are either themselves also distributed under Apache 2.0 license or distributed under a compatible license. 

Main author and contact: Patrice Lopez (<patrice.lopez@science-miner.com>)
