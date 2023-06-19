[![PyPI version](https://badge.fury.io/py/biblio_glutton_harvester.svg)](https://badge.fury.io/py/biblio_glutton_harvester)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# Open Access PDF harvester

Python utility for harvesting efficiently a very large Open Access collection of scholar PDF: 

* The downloaded PDF can be stored either on an Amazon S3 bucket, on a SWIFT object storage (OpenStack) or on a local storage, with UUID renaming/mapping. 

* Downloads and storage uploads over HTTP(S) are multi-threaded for best robustness and efficiency. 

* The download supports redirections, https protocol, wait/retries. It uses rotating request headers and supports Cloudflare protection via cloudscraper. 

* The harvesting process can be interrupted and resumed.

* The tool is fault tolerant, it will keep track of the failed resource access with corresponding errors and makes possible subsequent retry on this subset. 

* Optionally, aggregated metadata from biblio-glutton for an article are accessed and stored together with the full text resources. 

* As a bonus, image thumbnails of the front page of the PDF are created and stored with the PDF.

* It is also possible to harvest only a random sample of PDF instead of complete sets. 

The utility can be used in particular to harvest the full **Unpaywall** dataset (PDF) and the full **PMC** publications (PDF and corresponding NLM XML files).

## Requirements

The utility requires Python 3.6 or more. It is developed for a deployment on a POSIX/Linux server (it uses `imagemagick` to generate thumbnails, `gzip` and `wget` as external process). An S3 account and a dedicated S3 bucket or a SWIFT object storage and a dedicated SWIFT container must have been created for the cloud storage of the data collection. 

The utility will use some local storage dedicated to the embedded databases keeping track of the advancement of the harvesting, metadata and temporary downloaded resources. Consider a few GB of free space for a large scale harvesting of TB of PDF. 

__Storage__: as a rule of thumb, consider bewteen 1 and 1.5 TB for storage 1 million scholar PDF.

## Install

Get the github repo:

> git clone https://github.com/kermitt2/biblio-glutton-harvester

> cd biblio-glutton-harvester

It is advised to setup first a virtual environment to avoid falling into one of these gloomy python dependency marshlands:

> virtualenv --system-site-packages -p python3 env

> source env/bin/activate

Install the dependencies, use:

> python3 -m pip install -r requirements.txt

For generating thumbnails corresponding to the harvested PDF, ImageMagick must be installed. For instance on Ubuntu:

> apt-get install imagemagick

### Using PyPI package

PyPI packages are available for stable versions. Latest stable version is `0.2.0`:

```
python3 -m pip install biblio-glutton-harvester==0.2.0
```

## Configuration

A configuration file must be completed, by default the file `config.json` will be used, but it is also possible to use it as a template and specifies a particular configuration file when using the tool (via the `--config` parameter). 

- `data_path` is the path where temporary files, metadata and local DB are stored to manage the harvesting. If no cloud storage configuration is indicated, it is also where the harvested resources will be stored.  

- `compression` indicates if the resource files need to be compressed with `gzip` or not. Default is true, which means that all the harvested files will have an additional extension `.gz`. 

- `batch_size` gives the number of PDF that is considered for parallel process at the same time, the process will move to a new batch only when all the PDF of the previous batch will be processed.  
 
- `"prioritize_pmc"` indicates if the harvester has to choose a PMC PDF (NIH PMC or Europe PMC) when available instead of a publisher PDF, this can improve the harvesting success rate and performance, but depending on the task the publisher PDF might be preferred.  

- if a `biblio_glutton_base` URL service is provided, biblio-glutton will be used to enrich the metadata of every harvested articles. biblio-glutton provides aggregated metadata that extends CrossRef records with PubMed information and strong identifiers. 

- if a DOI is not found by `biblio_glutton`, it is possible to call the CrossRef REST API as a fallback to retrieve the publisher metadata. This is useful when the biblio-glutton service presents a gap in coverage for recent DOI records. 

- arXiv blocks machine-based harvesting of PDF, so it is necessary to create a local mirror of arXiv resources using dedicated data loader. If only PDF are required, see <https://github.com/kermitt2/arxiv_harvester> to create such mirror with PDF and metadata (a bit more than 2TB). For LaTeX sources, see [here](https://info.arxiv.org/help/bulk_data_s3.html#bulk-source-file-access) (around 3TB). The path to the arxiv resource mirror is set with the config parameter `arxiv_base`, an URI is expected and the path is resollved based on the arxiv ID as documented in the [arxiv_harvester](https://github.com/kermitt2/arxiv_harvester). 

```json
{
    "data_path": "./data",
    "compression": true,
    "batch_size": 100,
    "prioritize_pmc": false,
    "pmc_base": "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/",
    "biblio_glutton_base": "", 
    "crossref_base": "https://api.crossref.org",
    "crossref_email": "",
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "bucket_name": "",
    "region": "",
    "swift": {},
    "swift_container": "",
    "arxiv_base": "",
    "plos_base": "",
    "elife_base": ""
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
Important: It is assumed that the complete S3 bucket is dedicated to the harvesting. The `--reset` parameter will clear all the objects stored in the bucket, so be careful. 

The configuration for a SWIFT object storage uses the following parameters:

```json
{
    "swift": {},
    "swift_container": ""
}
```

If you are not using a SWIFT storage, remove these keys or leave these above values empty. Important: It is assumed that the complete SWIFT container is dedicated to the harvesting. The `--reset` parameter will clear all the objects stored in the container, so be careful. 

The `"swift"` key will contain the account and authentication information, typically via Keystone, something like this: 

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

Note: for harvesting PMC files, although the ftp server is used, the downloads tend to fail as the parallel requests increase. It might be useful to lower the default, and to launch `reprocess` for completing the harvesting. For the unpaywall dataset, we have good results with high `batch_size` (like 200), probably because the distribution of the URL implies that requests are never concentrated on one OA server. 

Also note that: 

* For PMC harvesting, the PMC fulltext available at NIH are not always provided with a PDF. In these cases, only the NLM file will be harvested.

* PMC PDF files can also be harvested via Unpaywall, not using the NIH PMC services. The NLM files will then not be included, but the PDF coverage might be better than a direct harvesting at NIH.

## Usage and options


```
usage: python3 OAHarvester.py [-h] [--unpaywall UNPAYWALL] [--pmc PMC] [--config CONFIG] [--dump DUMP]
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

The [Unpaywall database snapshot](https://unpaywall.org) is available from [OurResearch](http://ourresearch.org/). 

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

### Map for identifier mapping

A mapping with the UUID associated with an Open Access full text resource and the main identifiers of the entries can be dumped in JSONL (default file name is `map.jsonl`) with the following command:

```bash
> python3 OAHarvester.py --dump output.jsonl
```

By default, this map is always generated at the completion of an harvesting or re-harvesting. This mapping is necessary for further usage and for accessing resources associated to an entry (listing million files directly with AWS S3 is by far too slow, we thus need a local index/catalog).

In the JSONL dump, each entry identified as available Open Access is present with its UUID given by the attribute `id`, its main identifiers (`doi`, `pmid`, `pmcid`, `pii`, `istextId`), the list of available harvested resources and the target best Open Access URL considered.

```json
{"id": "00005fb2-0969-4ed6-92b3-0552f3fa283c", "doi": "10.1001/jamanetworkopen.2019.13325", "pmid": 31617925, "resources": ["json", "pdf"], "oa_link": "https://jamanetwork.com/journals/jamanetworkopen/articlepdf/2752991/ganguli_2019_oi_190509.pdf"}
```

The UUID can then be used for accessing the resources for this entry, the prefix path being based on the first 8 characters of the UUID, as follow: 

- PDF: `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.pdf`

- metadata in JSON: `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.json`

- possible JATS file (for harvested PMC full texts): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.nxml`

- thumbnail small (150px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-small.png`

- thumbnail medium (300px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-medium.png`

- thumbnail large (500px width): `1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2-thumb-large.png`

Note that if `"compression"` is set to `True` in the configuration file, __all these files__ will have a `.gz` extension.

Depending on the config, the resources can be accessed either locally under `data_path` or on AWS S3 following the URL prefix: `https://bucket_name.s3.amazonaws.com/`, for instance `https://bucket_name.s3.amazonaws.com/1b/a0/cc/e3/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2/1ba0cce3-335b-46d8-b29f-9cdfb6430fd2.pdf` - if you have set the appropriate access rights. The same applies to a SWIFT object storage based on the container name indicated in the config file. 

Only entries available in Open Access according to Unpaywall or PMC are present in the JSONL map file. If an entry is present in the JSONL map file but without a full text resource (`"pdf"` or "`"xml"`), it means that the harvesting of the Open Access file has failed. 

## Troubleshooting with imagemagick

A relatively recent update (end of October 2018) of imagemagick is breaking the normal conversion usage. Basically the converter does not convert by default for security reason related to server usage. For non-server mode as involved in our module, it is not a problem to allow PDF conversion. For this, simply edit the file 
` /etc/ImageMagick-6/policy.xml` and put into comment the following line: 

```
<!-- <policy domain="coder" rights="none" pattern="PDF" /> -->
```

## Building and deploying a Docker container

You need `docker` and `docker-compose` installed on your system. 

A `docker-compose.yml` file is available with the project, but you will need additionally:

- to update a configuration file according to your storage requirements (local, S3 or SWIFT)

- to create an external volume to store the embedded databases keeping track of the advancement of the harvesting, metadata and temporary downloaded resources, it's also on this external volume that the input file must be stored (the unpaywall dump file or the NIH PMC identifier list file) 

```console
docker-compose run --rm harvester
```

## License and contact

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0). The dependencies used in the project are either themselves also distributed under Apache 2.0 license or distributed under a compatible license. 

If you contribute to this Open Source project, you agree to share your contribution following this license. 

Main author and contact: Patrice Lopez (<patrice.lopez@science-miner.com>)
