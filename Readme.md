# Open Access PDF harvester

Python utility for harvesting efficiently a large Open Access collection of PDF: 

* Downloaded PDF can be stored either on an Amazon S3 bucket, on a SWIFT object storage (OpenStack) or on a local storage, with UUID renaming/mapping. 

* Downloads and storage uploads over HTTP(S) are multi-threaded for best robustness and efficiency. 

* Download supports redirections, https protocol and uses rotating request headers. 

* The harvesting process can be interrupted and resumed.

* The tool is fault tolerant, it will keep track of the failed resource access with corresponding errors and makes possible subsequent retry on this subset. 

* Optionally, aggregated metadata from biblio-glutton for an article are accessed and stored with the other resources. 

* As a bonus, image thumbnails of the front page of the PDF are created and stored with the PDF.

* It is also possible to harvest only a random sample of PDF instead of complete sets. 

The utility can be used in particular to harvest the **Unpaywall** dataset (PDF) and the **PMC** publications (PDF and corresponding NLM XML files).

## Requirements

The utility requires Python 3.6 or more. It is developed for a deployment on a POSIX/Linux server (it uses `imagemagick` as external process to generate thumbnails and `wget`). An S3 account and bucket or SWIFT object storage must have been created for non-local storage of the data collection. 

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

## Configuration

A configuration file must be completed, by default the file `config.json` will be used, but it is also possible to use it as a template and specifies a particular configuration file when using the tool. 

- In the configuration file, the information related to the S3 bucket to be used for uploading the resources must be filed, otherwise the resources will be stored locally in the indicated `data_path`. 

- `batch_size` gives the number of PDF that is considered for parallel process at the same time, the process will move to a new batch only when all the PDF of the previous batch will be processed.  
 
- if a `biblio_glutton_base` URL service is provided, biblio-glutton will be used to enrich the metadata of every harvested articles. biblio-glutton provides aggregated metadata that extends CrossRef records with PubMed information. 

- if a DOI is not found by `biblio_glutton`, it is possible to call the CrossRef REST API as a fallback to retrieve the metadata. This is useful when the biblio-glutton service presents a gap in coverage for the recent DOI records. 

```json
{
    "data_path": "./data",
    "batch_size": 100,
    "pmc_base": "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/",
    "biblio_glutton_base": "", 
    "crossref_base": "https://api.crossref.org",
    "crossref_email": "",
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "bucket_name": "",
    "region": "",
    "swift": {},
    "swift_container": ""
}
```

Configuration for a S3 storage uses the following parameters:

```json
{
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "bucket_name": "",
    "region": ""
}
```
If you are not using a S3 storage, remove these keys or leave these values empty.

The configuration for a SWIFT object storage uses the following parameters:

```json
{
    "swift": {},
    "swift_container": ""
}
```

If you are not using a SWIFT storage, remove these keys or leave these above values empty. The `"swift"` key will contain the account and authentication information, typically via Keystone, something like this: 

```json
{
    "swift": {
        "auth_version": "3",
        "auth_url": "https://auth......./v3",
        "os_username": "user-007",
        "os_password": "1234",
        "os_user_domain_name": "Default",
        "os_project_domain_name": "Default",
        "os_project_name": "myProjectName",
        "os_project_id": "myProjectID",
        "os_region_name": "NorthPole",
        "os_auth_url": "https://auth......./v3"
    },
    "swift_container": "my_glutton_oa_harvesting"
}
```

Note: for harvesting PMC files, although the ftp server is used, downloads tend to fail as the parallel requests increase. It might be useful to lower the default, and to launch `reprocess` for completing the harvesting. For the unpaywall dataset, we have good results with high `batch_size` (like 200), probably because the distribution of the URL implies that requests are never concentrated on one server. 

Also note that: 

* The PMC fulltext available at NIH are not always provided with a PDF. In these cases, only the NLM file will be harvested.

* PMC PDF files can also be harvested via Unpaywall, not using the NIH PMC services. The NLM files will then not be included, but the PDF coverage might be better than a direct harvesting at NIH.

## Usage and options


```
usage: OAHarvester.py [-h] [--unpaywall UNPAYWALL] [--pmc PMC] [--config CONFIG] [--dump DUMP]
                      [--reprocess] [--reset] [--thumbnail] [--sample SAMPLE]

Open Access PDF harvester

optional arguments:
  -h, --help            show this help message and exit
  --unpaywall UNPAYWALL
                        path to the Unpaywall dataset (gzipped)
  --pmc PMC             path to the pmc file list, as available on NIH's site
  --config CONFIG       path to the config file, default is ./config.json
  --dump DUMP           write a map with UUID, article main identifiers and available harvested
                        resources
  --reprocess           reprocessed failed entries with OA link
  --reset               ignore previous processing states, clear the existing storage and re-
                        init the harvesting process from the beginning
  --thumbnail           generate thumbnail files for the front page of the PDF
  --sample SAMPLE       Harvest only a random sample of indicated size

```

The Unpaywall dataset is available from Impactstory. 

`PMC_FILE_LIST` can currently be accessed as follow:
- all OA files: ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt
- non commercial-use OA files: ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_non_comm_use_pdf.txt
- commercial-use OA files (CC0 and CC-BY): ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_comm_use_file_list.txt


For processing all entries of an Unpaywall snapshot:

```bash
> python3 OAHarvester.py --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

By default, no thumbnail images are generated. For generating thumbnail images from the front page of the downloaded PDF (small, medium, large):

```bash
> python3 OAHarvester.py --thumbnail --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz 
```

By default, `./config.json` is used, but you can pass a specific config with the `--config` option:

```bash
> python3 OAHarvester.py --config ./my_config.json --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

If the process is interrupted, relaunching the above command will resume the process at the interruption point. For re-starting the process from the beginning, and removing existing local information about the state of process, use the parameter `--reset`:

```bash
> python3 OAHarvester.py --reset --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

After the completion of the snapshot, we can retry the PDF harvesting for the failed entries with the parameter `--reprocess`:

```bash
> python3 OAHarvester.py --reprocess --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

For downloading the PDF from the PMC set, simply use the `--pmc` parameter instead of `--unpaywall`:

```bash
> python3 OAHarvester.py --pmc /mnt/data/biblio/oa_file_list.txt
```

For harvesting only a predifined random number of entries and not the whole sets, the parameter `--sample` can be used with the desired number:

```bash
> python3 OAHarvester.py --pmc /mnt/data/biblio/oa_file_list.txt --sample 2000
```

This command will harvest 2000 PDF randomly distributed in the complete PMC set. For the Unpaywall set, as around 20% of the entries only have an Open Access PDF, you will need to multiply by 5 the sample number, e.g. if you wish 2000 PDF, indicate `--sample 10000`. 


### Dump for identifier mapping

A mapping with UUID and the main identifiers of the entries can be dumped in JSONL (default file name is `map.jsonl`) with the following command:

```bash
> python3 OAHarvester.py --dump output.jsonl
```

This dump is necessary for further usage and for accessing resources associated to an entry (listing million files directly with AWS S3 is by far too slow, we thus need a local index and a DB).

In the JSONL dump, each entry is present in the dump with its UUID given by the attribute `id`, its main identifiers (`doi`, `pmid`, `pmcid`, `pii`, `istextId`) and the list of available harvested resources.

```json
{ 
    "id": "904689c2-b25b-452d-a38e-81114e7509bf", "doi": "10.1001/jama.2016.1952", "pmid": 26978209, "resources": ["json", "pdf"]
}
```

The UUID can then be used for accessing the resources for this entry, the prefix path being based on the first 8 characters of the UUID, as follow: 

- PDF: `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.pdf`

- metadata in JSON: `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.json`

- possible JATS file (for harvested PMC full texts): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.nxml`

- thumbnail small (150px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-small.png`

- thumbnail medium (300px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-medium.png`

- thumbnail large (500px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-large.png`

Depending on the config, the resources can be accessed either locally under `data_path` or on AWS S3 following the URL prefix: `https://bucket_name.s3.amazonaws.com/`, for instance `https://bucket_name.s3.amazonaws.com/1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.pdf` - if you have set the appropriate access rights. The same applies to SWIFT object storage based on the container name indicated in the config file. 

## Troubleshooting with imagemagick

Relatively recent update (end of October 2018) of imagemagick is breaking the normal conversion usage. Basically the converter does not convert by default for security reason related to server usage. For non-server mode as involved in our module, it is not a problem to allow PDF conversion. For this, simply edit the file 
` /etc/ImageMagick-6/policy.xml` and put into comment the following line: 

```
<!-- <policy domain="coder" rights="none" pattern="PDF" /> -->
```

## License and contact

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0). The dependencies used in the project are either themselves also distributed under Apache 2.0 license or distributed under a compatible license. 

If you contribute to this Open Source project, you agree to share your contribution following this license. 

Main author and contact: Patrice Lopez (<patrice.lopez@science-miner.com>)
