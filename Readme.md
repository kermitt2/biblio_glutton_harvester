[![PyPI version](https://badge.fury.io/py/biblio_glutton_harvester.svg)](https://badge.fury.io/py/biblio_glutton_harvester)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# Open Access harvester

This tool is a Python utility for harvesting efficiently a very large Open Access collection of scholar PDF, XML and metadata, from the Unpaywall dataset, from PubMed Central or from a given list of DOI: 

* The downloaded PDF or XML can be stored either on an Amazon S3 bucket, on a SWIFT object storage (OpenStack) or on a local storage, with UUID renaming/mapping. 

* Downloads and storage uploads over HTTP(S) are multi-threaded for best robustness and efficiency. 

* The download supports redirections, https protocol, wait/retries. It uses rotating request headers and supports Cloudflare protection via cloudscraper. 

* The harvesting process can be interrupted and resumed.

* The tool is fault tolerant, it will keep track of the failed resource access with corresponding errors and makes possible subsequent retry on this subset. 

* Optionally, aggregated metadata from biblio-glutton for an article are accessed and stored together with the full text resources. 

* Mirrors of pre-downloaded/dump resources can be used as the harvesting is performed for higher download rate for arXiv and PLOS resources.

* As a bonus, image thumbnails of the front page of the PDF can be created and stored with the PDF.

* It is also possible to harvest only a random sample of PDF or just a list of DOIs instead of complete sets. 

* For full texts present on PubMed Central, JATS XML can be downloaded complementary to PDF. Similarly, if an arXiv source mirror is provided, the PDF and LaTex source from arXiv will be harvested. 

The utility can be used in particular to harvest the full **Unpaywall** dataset (PDF) and the full **PMC** publications (PDF and corresponding JATS/NLM XML files). The tool is designed to scale to several ten million of full text and metadata downloads.

## Requirements

The utility requires Python 3.6 or more. It is developed for a deployment on a POSIX/Linux server (it uses `imagemagick` to generate thumbnails, `gzip` and `wget` as external process). An S3 account and a dedicated S3 bucket or a SWIFT object storage and a dedicated SWIFT container must have been created for the cloud storage of the data collection. 

The utility will use some local storage dedicated to the embedded databases keeping track of the advancement of the harvesting, metadata and temporary downloaded resources. Consider a few GB of free space for a large scale harvesting of TB of PDF. 

__Storage__: as a rule of thumb, consider bewteen 1 and 1.5 TB for storing 1 million scholar PDF. As of May 2023, we harvested 35M full texts for the full Unpaywall collection, which takes around 45TB storage space. 

## Install

Get the github repo:

```console
git clone https://github.com/kermitt2/biblio_glutton_harvester
cd biblio_glutton_harvester
```

It is advised to setup first a virtual environment to avoid falling into one of these gloomy python dependency marshlands:

```console
virtualenv --system-site-packages -p python3 env
source env/bin/activate
```

Install the dependencies and the project:

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
```

For generating thumbnails corresponding to the harvested PDF, ImageMagick must be installed. For instance on Ubuntu:

```console
apt-get install imagemagick
```

### Using PyPI package

PyPI packages are available for stable versions. Latest stable version is `0.2.3`:

```
python3 -m pip install biblio_glutton_harvester==0.2.3
```

## Configuration

A configuration file must be completed, by default the file `config.yaml` will be used, but it is also possible to use it as a template and specifies a particular configuration file when using the tool (via the `--config` parameter). 

- `data_path` is the path where temporary files, metadata and local DB are stored to manage the harvesting. If no cloud storage configuration is indicated, it is also where the harvested resources will be stored.  

- `compression` indicates if the resource files need to be compressed with `gzip` or not. Default is true, which means that all the harvested files will have an additional extension `.gz`. 

- `batch_size` gives the maximum number of parallel tasks (download, storage, compression, validation, ...) performed at the same time, the process will move to a new batch only when all the PDF and metadata of the previous batch have been harvested and validated.  
 
- `cloudflare_support` (`true` or `false`, default is `false`) indicates if cloudscraper should be used to manage download following cloudflare challenge(s), this will slow down very significantly the average download time, but should provide a higher download success rate.

The `resources` part of the configuration indicates how to access PubMed Central (PMC), arXiv and PLOS resources. 

- For PMC, `prioritize_pmc` indicates if the harvester has to choose a PMC PDF (NIH PMC or Europe PMC) when available instead of a publisher PDF, this can improve the harvesting success rate and performance, but depending on the task the publisher PDF might be preferred. The `pmc_base` is normally the NIH FTP address where to find the PDF and full text JATS. 

- For arXiv, we indicate a possible mirror on a S3 compatible storage, as created with [arxiv_harvester](https://github.com/kermitt2/arxiv_harvester) (a bit more than 2TB for PDf and metadata only, 3TB more with LaTeX sources). Note that the path to individual resources is resolved based on the arxiv ID as documented in [arxiv_harvester](https://github.com/kermitt2/arxiv_harvester). The LaTeX sources of the arXiv articles can be added to this arXiv mirror and will be harvested together with the PDF. The LaTeX source tar gzipped file of a given resource must be under the same path as the PDF. See (here)[https://info.arxiv.org/help/bulk_data_s3.html] about how to copy the arXiv LaTeX sources on S3 (it will cost around $500 as AWS requester S3 bucket fees). 

- For PLOS, if the "All Of PLOS" collection have been downloaded (around 330K JATS files), we indicate a possible mirror on a S3 compatible storage (see the zpped dump at <https://plos.org/text-and-data-mining/> or <https://github.com/PLOS/allofplos>).

In `metadata` part of the configuration:

- if a `biblio_glutton_base` URL service is provided, biblio-glutton will be used to enrich the metadata of every harvested articles. [biblio-glutton](https://github.com/kermitt2/biblio-glutton) provides aggregated metadata that extends CrossRef records with PubMed information and strong identifiers. 

- if a DOI is not found by `biblio_glutton`, it is possible to call the CrossRef REST API as a fallback to retrieve the publisher metadata. This is useful when the biblio-glutton service presents a gap in coverage for recent DOI records. 

The configuration for a compatible S3 storage uses the `aws` section of the configuration. If Amazon AWS S3 service is used, leave the `aws_end_point` empty. If you are using an alternative compatible S3 service, you must indicate the end point in the `aws_end_point` parameter. If you are not using a S3 storage, remove the related related or leave these values empty.

Important: It is assumed that the complete S3 bucket is dedicated to the harvesting. The `--reset` parameter will clear all the objects stored in the bucket, so be careful. 

The configuration for an OpenStack SWIFT object storage uses `swift` section of the configuration. If you are not using a SWIFT storage, remove the related parameters or leave these values empty. Important: It is assumed that the complete SWIFT container is dedicated to the harvesting. The `--reset` parameter will clear all the objects stored in the container, so be careful. 

The `"swift"` key will contain the account and authentication information, typically via Keystone. 

Note: for harvesting PMC files, although the ftp server is used, the downloads tend to fail as the parallel requests increase. It might be useful to lower the default, and to launch `reprocess` for completing the harvesting. For the unpaywall dataset, we have good results with high `batch_size` (like 200), probably because the distribution of the URL implies that requests are never concentrated on one OA server. However, `batch_size` at 100 is more conservative in general and should give higher download rate, and if only PMC files are downloaded `batch_size` at 20 is recommended. 

Also note that: 

* For PMC harvesting, the PMC fulltext available at NIH are not always provided with a PDF. In these cases, only the NLM file will be harvested.

* PMC PDF files can also be harvested via Unpaywall, not using the NIH PMC services. The NLM files will then not be included, but the PDF coverage might be better than a direct harvesting at NIH only.

## Usage and options


```
usage: python3 -m biblio_glutton_harvester.OAHarvester [-h] [--unpaywall UNPAYWALL] [--pmc PMC] [--config CONFIG] [--dump DUMP]
                      [--reprocess] [--reset] [--thumbnail] [--sample SAMPLE]

Open Access PDF harvester

optional arguments:
  -h, --help            show this help message and exit
  --unpaywall UNPAYWALL
                        path to the Unpaywall dataset (gzipped)
  --pmc PMC             path to the pmc file list, as available on NIH's site
  --config CONFIG       path to the config file, default is ./config.yaml
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
> python3 -m biblio_glutton_harvester.OAHarvester --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

By default, no thumbnail images are generated. For generating thumbnail images from the front page of the downloaded PDF (small, medium, large):

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --thumbnail --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz 
```

By default, `./config.json` is used, but you can pass a specific config with the `--config` option:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --config ./my_config.json --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

If the process is interrupted, relaunching the above command will resume the process at the interruption point. For re-starting the process from the beginning, and removing existing local information about the state of process, use the parameter `--reset`:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --reset --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

After the completion of the snapshot, we can retry the PDF harvesting for the failed entries with the parameter `--reprocess`:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --reprocess --unpaywall /mnt/data/biblio/unpaywall_snapshot_2018-06-21T164548_with_versions.jsonl.gz
```

For downloading the PDF from the PMC set, simply use the `--pmc` parameter instead of `--unpaywall`:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --pmc /mnt/data/biblio/oa_file_list.txt
```

For harvesting only a predifined random number of entries and not the whole sets, the parameter `--sample` can be used with the desired number:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --pmc /mnt/data/biblio/oa_file_list.txt --sample 2000
```

This command will harvest 2000 PDF randomly distributed in the complete PMC set. For the Unpaywall set, as around 20% of the entries only have an Open Access PDF, you will need to multiply by 5 the sample number, e.g. if you wish 2000 PDF, indicate `--sample 10000`. 

### Map for identifier mapping

A mapping with the UUID associated with an Open Access full text resource and the main identifiers of the entries can be dumped in JSONL (default file name is `map.jsonl`) with the following command:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --dump output.jsonl
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

## Converting the PDF files into XML TEI

[GROBID](https://github.com/kermitt2/grobid) is a service developed to structure automatically scholar PDF into XML TEI files thanks to Machine Learning techniques. First, you will need a Grobid service installed and running. We recommand using a [Docker container](https://grobid.readthedocs.io/en/latest/Grobid-docker/) to simplify the installation and deployment of the server. Second, we recommand using the [Grobid Python client](https://github.com/kermitt2/grobid_client_python) to process at scale the harvested PDF. The client will process in an efficient concurrent manner the PDF in the `data_path` directory.

## Converting the PMC XML JATS files into XML TEI

After the harvesting realised by `biblio_glutton_harvester.OAHarvester`, it is possible to convert efficiently of downloaded PMC XML JATS files into XML TEI. This will provide better XML quality than what can be extracted automatically by Grobid from the PDF. This conversion allows to have all the documents in the same XML TEI customization format. As the TEI format superseeds JATS, there is no loss of information from the JATS file. It requires [Pub2TEI](https://github.com/kermitt2/Pub2TEI) to be installed and the path to Pub2TEI `pub2tei_path` to be set in the `config.yaml` file of the `biblio_glutton_harvester` project.

To launch the conversion under the default `data/` directory:

```console
python3 -m biblio_glutton_harvester.nlm2tei
```

If a custom config file and custom `data/` path are used:

```console
python3 -m biblio_glutton_harvester.nlm2tei --config ./my_config.yaml
```

This will apply Pub2TEI (a set of XSLT) to all the harvested `*.nxml` files and add to the document repository a new file TEI file, for instance for a CORD-19 entry:

```
00/0a/je/vz/000ajevz/000ajevz.pub2tei.tei.xml
```

Note 1: Pub2TEI supports a lot of other publisher's XML formats (and variants of these formats), so the principle and current tool could be used to transform different publisher XML formats into a single one (TEI) - not just NLM/JATS, facilitating and centralizing further ingestion and process by avoiding to write complicated XML parsers for each case. 

Note 2: It is expected to get `8 transformations failed` at the end of the process, these "failed" transformations correspond to temporary empty DTD added to avoid loading DTD online for each input XML document.  

## Converting the LaTeX source files into XML TEI

After the harvesting realised by `biblio_glutton_harvester.OAHarvester`, it is possible to convert the downloaded LaTeX source files into XML TEI. These source files come typically from arXiv. This will provide better XML quality than what can be extracted automatically by Grobid from the PDF. This conversion allows to have all the documents in the same XML TEI customization format. For best TEI conformance, it requires the forked [LaTeXML](https://github.com/kermitt2/LaTeXML) to be installed and the path to LaTeXML `latexml_path` to be set in the `config.yaml` file of the `biblio_glutton_harvester` project.

To launch the conversion under the default `data/` directory:

```console
python3 -m biblio_glutton_harvester.latex2tei
```

If a custom config file and custom `data/` path are used:

```console
python3 -m biblio_glutton_harvester.latex2tei --config ./my_config.yaml
```

This will apply LaTeXML to all the harvested `*.zip` files, examine the `.tex` files, identify the root latex file, convert and finally add the converted TEI XML file in the document repository, similarly as other resources. The extension for TEI XML files generated from the LaTeX source is `.latex.tei.xml`, for example:

```
ea/53/8f/ec/ea538fec-f7ec-4119-bcab-7362a47b31b6/ea538fec-f7ec-4119-bcab-7362a47b31b6.latex.tei.xml
```

## Harvesting from a list of DOI

The tool has been designed first for mass harvesting of full texts from the Unpaywall dataset or from PubMed Central. However, it can also be used from a list of DOI to donwload and an Unpaywall dump. The list of DOI to harvest must be provided in a file, with one DOI per line. The following script will generate the subset of the Unpaywall dataset for this list of DOI:

```
usage: unpaywall_preprocess_selection.py [-h] [--unpaywall UNPAYWALL] [--dois DOIS] [--output OUTPUT]

Open Access PDF harvester

optional arguments:
  -h, --help            show this help message and exit
  --unpaywall UNPAYWALL
                        path to the Unpaywall dataset (gzipped)
  --dois DOIS           path to the list of DOIs to be used to create the Unpaywall subset
  --output OUTPUT       where to write the subset Unpaywall file, a .json.gz extension file
```

For example, with a file of DOI (one DOI per line) called `dois.txt`:

```console
python3 biblio_glutton_harvester/unpaywall_preprocess_selection.py --unpaywall unpaywall_snapshot_2023-11-12T083002.jsonl.gz --dois dois.txt --output dois-unpaywall.json.gz
```

The generated file `dois-unpaywall.json.gz` is the unpaywall subset corresponding to the list of DOI to donwload, which can then be used with the main harvesting command:

```bash
> python3 -m biblio_glutton_harvester.OAHarvester --unpaywall dois-unpaywall.json.gz
```

## Troubleshooting with imagemagick

A relatively recent update (end of October 2018) of imagemagick is breaking the normal conversion usage. Basically the converter does not convert by default for security reason related to server usage. For non-server mode as involved in our module, it is not a problem to allow PDF conversion. For this, simply edit the file 
` /etc/ImageMagick-6/policy.xml` and put into comment the following line: 

```
<!-- <policy domain="coder" rights="none" pattern="PDF" /> -->
```

## License and contact

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0). The dependencies used in the project are either themselves also distributed under Apache 2.0 license or distributed under a compatible license. 

If you contribute to this Open Source project, you agree to share your contribution following this license. 

Main author and contact: Patrice Lopez (<patrice.lopez@science-miner.com>)
