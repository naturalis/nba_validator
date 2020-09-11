# NBA JSON-document validator

## Environment variables (in .env file)
```SHELL
# all paths relative to container mappings!

# folder `import datasets`-process looks in for prepared jobs
INCOMING_JOB_FOLDER=/data/incoming_jobs

# folder `import datasets` and `create dataset`-processes write validator-ready jobs to
VALIDATOR_JOB_FOLDER=/data/job_to_be_validated

# folder validator writes validated jobs to
OUTGOING_JOB_FOLDER=/data/validated_jobs

# folder validator writes validated data files to
OUTGOING_OUTPUT_FOLDER=/data/validated_data

# folder validator writes processed test jobs to
OUTGOING_TEST_JOB_FOLDER=/data/test_jobs

# output folder for the validator (intermediate folder when running jobs)
VALIDATED_OUTPUT_FOLDER=/data/validator_output

# folder validator writes failed jobs to
OUTGOING_FAILED_JOBS_FOLDER=/data/failed_jobs/

# folder with data supplier configurations
# (usually a local checkout of https://github.com/naturalis/nba_validator_config/)
INI_FILE_FOLDER=/config

# mapping of supplier to configurations-file
INI_FILE_LIST={"BRAHMS":"brahms.ini","COL":"col.ini","CRS":"crs.ini","CSR":"csr.ini","GEO":"geoareas.ini","NSR":"nsr.ini","OBS":"obs.ini","XC":"xenocanto.ini"}

# general log file path
LOG_FILE=/log/validator.log

# alternative temp-folder (optional, defaults to system default)
TMP_FOLDER=/data/temporary

# when processing, free disk space must be larger than (cumulative job size * JOB_DISK_USAGE_FACTOR) (optional, defaults to 3)
JOB_DISK_USAGE_FACTOR=2

SLACK_ENABLED=0
#SLACK_WEBHOOK=<slack webhook>
#OUTFILE_LINES=500000
```


## Running

### Importing datasets
```SHELL
docker run --env-file .env \
    -v /data/validator:/data \
    -v /data/validator/log:/log \
    -v /data/validator/nba_validator_config:/config \
    -it naturalis/nba-validator:latest php import_datasets.php
```
The program chooses the INI-file from the .ENV-variable `INI_FILE_LIST`, using the field `data_supplier` in the job-file as index.

### Creating a dataset (NSR example)
```SHELL
docker run --env-file .env \
    -e SUPPPLIER_CONFIG_FILE=/config/nsr.ini \
    -e FORCE_DATA_REPLACE=0 \
    -e FORCE_TEST_RUN=0 \
    -v /data/validator:/data \
    -v /data/validator/log:/log \
    -v /data/validator/nba_validator_config:/config \
    -it naturalis/nba-validator:latest php create_dataset.php
```
`FORCE_DATA_REPLACE` ("tabula rasa") and `FORCE_TEST_RUN` are optional and both default to false. All supplier specific settings and folders - such as incoming data and reports folders - are in the INI-file.

### Processing datasets
```SHELL
docker run --env-file .env \
    -v /data/validator:/data \
    -v /data/validator/log:/log \
    -v /data/validator/nba_validator_config:/config \
    -v /data/validator/nba_json_schemas:/schemas \
    -it naturalis/nba-validator:latest php process_datasets.php
```