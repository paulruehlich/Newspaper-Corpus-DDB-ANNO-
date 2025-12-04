# Newspaper-Corpus-DDB-ANNO-
Reproducible data extraction pipeline for ANNO &amp; DDB newspapers (here: 1871–1954).
This repository contains the complete and reproducible codebase used to construct a historical newspaper corpus covering the years 1871–1954 from two major European digital newspaper archives:

  ANNO (Austrian Newspapers Online, Österreichische Nationalbibliothek)
  Deutsche Digitale Bibliothek – Zeitungsportal
  
The repository provides only the code necessary to reproduce the pipeline.
The resulting datasets (OCR text at page level) are not included here due to licensing and storage constraints.
External download links will be provided in the final publication.

1. Purpose of the Repository
    The scripts in this repository were used to:
    Collect issue-level and page-level metadata.
    Select suitable newspapers for corpus construction based on temporal coverage and data availability.
    Retrieve or scrape OCR full text.
    Preprocess and merge all extracted data into standardized formats.
    Ensure full reproducibility for a scientific data paper.
    All scripts reproduce the extraction procedure exactly as it was implemented in the original project.

2. Repository Structure

ANNO/
   01metadata_generator_cities_timespan.py
   02newspaper_selection_anno.py
   03anno_worker_module.py
   04run_anno_workers.py
   05merging_tool_worker_output.py

DDB/
   01newspaper_timespan_generator.py
   02newspaper_selection.py
   03newspaper_api_access.py


3. ANNO Pipeline (Austria)
   
  3.1 Overview

    The ANNO pipeline retrieves newspaper metadata and full‐text page images by means of structured scraping.
    ANNO provides no public API for full OCR access; therefore, a parallelized scraping system was implemented.
  
  3.2 Script Description

   01metadata_generator_cities_timespan.py
   
    - Generates a complete list of all newspaper issues published in Vienna from 1871 to 1954.
    - Outputs a CSV containing aid, title, and issue dates (YYYYMMDD).
    - This file serves as the metadata basis for all subsequent ANNO processing.
  
  02newspaper_selection_anno.py
    
    Filters the metadata to identify suitable newspaper families.
    Selection is based on:
       - temporal coverage across the period 1871–1954
       - continuity of publication
       - availability of scanned issues
       - balancing of early and late coverage across titles
    The result is a reduced metadata file containing the selected newspapers.
    
  03anno_worker_module.py

    Core scraping module for ANNO page-level OCR retrieval.
    Includes:
      - request handling,
      - polite rate limiting,
      - retries and error recovery,
      - proxy routing (Decodo IP rotation),
      - page parsing,
      - checkpointing of progress.
    Each page is extracted and saved with metadata including title, issue date, page index, and OCR text.

  04run_anno_workers.py

    Executes the scraping module in parallel using Python's multiprocessing library.
    Distributes issue subsets across multiple worker processes to accelerate full-corpus extraction.
    Implements orderly startup, completion tracking, and failure handling.
    
  05merging_tool_worker_output.py
    
    Consolidates all partial outputs created by the workers.
    Ensures:
      - proper ordering by date and page number,
      - deduplication,
      - normalization of column names,
      - final serialization into a single dataset.
    This script produces the final ANNO page-level corpus.

    
  
  4. DDB Pipeline (Germany)

     4.1 Overview

         The DDB Zeitungsportal provides structured metadata and OCR text via an API.
         The extraction relies on the official ddbapi wrapper, enabling stable and reproducible access.

     4.2 Script Description
     
        01newspaper_timespan_generator.py
     
         Queries all newspaper titles available in the DDB Zeitungsportal.
         Parses the progress metadata field to compute start and end years of OCR availability for each title.
         Outputs zeitungszeiträume_alle.csv, containing:
            - DDB ID
            - title
            - start year
            - end year
            - number of years covered
     
      02newspaper_selection.py

         Filters the time span table to select six target newspapers.
         The selection criteria mirror those used for ANNO:
          - long and stable temporal coverage,
          - coverage anchored around the period 1871–1954,
          - plausibility of OCR completeness,
          - balancing of early and late publications.
         Results in a list of selected newspapers for extraction.
    
      03newspaper_api_access.py

          Retrieves issue-level and page-level OCR text for the selected newspapers.
          Process:
            - Issues are retrieved using zp_issues to determine all available ZDB identifiers.
            - For each ZDB identifier, all pages are retrieved using zp_pages.
            - Page-level metadata (publication date, pagenumber, text) is extracted.
            - Data are filtered to the period 1871–1954.
            - Additional preprocessing includes:
                 - parsing publication dates into year, month, day
                 - ordering pages chronologically
                 - restricting page numbers where required
            - The script outputs:
                 - a CSV per newspaper
                 - a consolidated master file
          This constitutes the final DDB page-level corpus.
     
5. External Data Availability
   
       The extracted full-text datasets are too large to store directly in this repository and are additionally restricted by the licensing terms of ANNO and        the DDB.
       A stable external download link (e.g., Zenodo or Dropbox) will be included in the final publication of the associated data paper.
   
6. Dependencies

   The scripts require Python 3.9 or later and the following packages:
      - pandas
      - requests
      - bs4 (BeautifulSoup)
      - ddbapi
      - pathlib
      - re

7. Execution Notes
   
  API keys must be exported before running DDB scripts:
  
      export DDB_API_KEY="YOUR_KEY"

  Proxy credentials must be exported before running ANNO scraping:
      
      export DECODO_USER="…"
      export DECODO_PASS="…"

  ANNO scraping is compute‐ and bandwidth‐intensive and may take several hours, depending on proxy throughput.
  DDB page-level extraction retrieves all pages for each ZDB identifier before filtering because the Zeitungsportal API does not support server-side            filtering by publication date.
  Some scripts assume UNIX-style paths; Windows users may need to adjust file separators.




