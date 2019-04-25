# PHP JSON Schema validator

This validator validates JSON documents against JSON-schema's as found elsewhere in this repo.

## Requirements
PHP7, Composer, SQLite3 (PHP library), jq  
Make sure the BCMath extension is enabled for PHP (<http://php.net/manual/en/bc.installation.php>).  
The application uses JSON Guard as JSON validator (http://json-guard.thephpleague.com). Composer will take care of the JSON Guard dependency.































## Using JsonValidator

Include `class.json-validator.php` in your script.

Instantiate the class as follows:

```php
$jsonValidator = new JsonValidator([
	'output_dir'=>$output_dir,	# folder to write valid JSON documents to
	'schema_file'=>$schema_file	# path to the JSON schema file to validate against
	'save_file_basename' => $save_file_basename	# base name for the output file(s)
]);

```
Set code and name for the data supplier (optional):
```php
$jsonValidator->setSourceSystemDefaults(["source_system_name"=>"Data Supplier 1","source_system_code"=>"DS1","source_institution_id"=>"DS-1","source_id"=>"DS-1" ]);
```
These will be used to overwrite the values of the elements of `sourceSystem.code`, `sourceSystem.name` and `sourceInstitutionID` in all valid documents. This is a security measure against typo's etc. made by the data supplier, `sourceSystem` being a key field. Please note that the fields still have to be present in all JSON docs.

Other settings (see 'Complete configuration overview' for explanation):
```php
$jsonValidator->setLoadErrorThreshold(100);
$jsonValidator->setReadBufferSize(10000);
$jsonValidator->setIdElementName("myID");
$jsonValidator->setAllowDoubleIds(true);
```
Adding files to validate:
```php
$jsonValidator->addFileToValidate( "/path/to/my/file.json" );
```

Run the validator:
```php
$jsonValidator->run();
```
The application will read the input files line by line. Input files are expected to have one vallid JSON-doc per line. When it has reached 500.000 lines, it will validate them against the schema. Lines that cannot be interpreted as JSON are omitted. After having been validated, valid JSON-documents are written to an output file in the output directory. The application then moves on to the next 500.000 and so on, until all lines in all files in the input directory have been read.

Errors are written to a separate file in the output folder.

To get an overview of the validation results, call:
```php
$results = $jsonValidator->getValidationOverview();
```
and print the results.

To print the used settings, call
```php
$settings = $jsonValidator->getSettingsOverview();
```
and print the results.


## Input file format
The validator expects the input files to contain one JSON file per line. Only files with the extension .json are processed. Technically speaking, files containing multiple separate JSON-documents do not themselves qualify as legal JSON-files. However, since the practice is widespread, it has become a de facto standard. Input files that are formatted as a single, multi-line document fail validation, both if it's a single document and if it's an array of multiple single line documents.


## Complete configuration overview

When instantiating the JsonValidator-class, a parameter is required that contains the following elements:
- `output_dir`: destination directory for output and error files (mandatory, dir must exist)
- `schema_file`: JSON-schema file to use for validation (mandatory, file must exist)
- `save_file_basename` | basename used for all output files (optional, defaults to _validation-output_; timestamp is added automatically)

Potentially, there are four types of output files:
- valid output (one JSON document per line) (example `validation-output-20180704-133717--000.json`)
- broken documents (lines that didn't parse; invalid JSON) (example `validation-output-20180704-133717--broken--000.json`)
- invalid documents (lines that did parse, but didn't pass the validation) (example `validation-output-20180704-133717--invalid--000.json`)
- errors (a list of errors) (example `validation-output-20180704-133717--errors--000.json`)

Rollover is 500000 lines per file for all files (constant; JsonValidator::LINES_PER_OUTPUT_FILE), except the file with valida JSON-documents. The max length of that file can be controlled with `$jsonValidator->setMaxOutfileLength()` (defaults to 0 = single file). 

Other paramters and their setters (includes info on the entire validation process as well, so do read):

**`setSourceSystemDefaults( $p )`**
Method to set defaults for four specific fields within all valid JSON-document. These fields are considered to be fundamental to the logical integrity of the NDS, and their value can be overwritten with the correct values for a specific data supplier. The fields and their corresponding JSON-elements are:
- `source_system_code` : sourceSystem.&#8203;code
- `source_system_name` : sourceSystem.&#8203;name
- `source_id` : sourceID
- `source_institution_id` : sourceInstitutionID
Setting `$p` to _false_ leaesng the supplied values intact (which is also the default). You can supply a subset by omitting the variables whose value you want to keep (omit completely, or set the value to _null_; setting value to "" will raise an error). Adding other parameters to overwrite other fields has no effect (and are silently ignored). Supplied values are checked against the validation schema, so typo's won't silently propagate to the NDS.

**`setIdElementName($name)`**
Method to set the name of the element in the documents that holds the unique ID value. Used for reporting functions. Defaults to _id_.

**`setReadBufferSize($read_buffer_size)`**
Method to set the number of lines to read from the input files before passing them on to the validator. The validator passes through a "read - validate - check for double ID's - write"-cycle every `read_buffer_size` number of lines. Default is _500000_, set to something smaller if you run out of memory, or if you want the double ID check to happen more often. The read-phase passes over input file ends and beginnings, so the size and content of the output files do not necessarily correspond to those of the input files.

**`setLoadErrorThreshold($threshold)`**
Method to set the load error threshold. If all first `$load_error_threshold` lines fail (either becuase they are broken or invalid), the entire process aborts. This is to protect against trying to load millions of lines that all have errors (especially useful the first time you get files from a new data supplier). Set to 0 to disable (default).

**`setAllowDoubleIds($state)`**
Duplicate ID's can cause differences between numbers of supplied and loaded documents that are hard to trace. This settings controls the monitoring of duplicate ID's. `setAllowDoubleIds` toggles *strict* monitoring of duplicates (default _true_, allowing duplicate ID's). If set to _false_, the validator will track the number of unique ID's (using the element set in `setIdElementName`; if no valid ID-element is set, the validator behaves as if `setAllowDoubleIds` is _true_) in the import. If it finds that number is smaller than the total number of ID's read, it aborts the entire process. This check is executed once every read-validate-write cycle. Halting because of duplicate ID's will occur regardless of the values of `fail_on_any_error` (see `setFailOnAnyError($state)`).

**`setWriteBrokenAndInvalid($state)`**
Method to set whether broken and invalid documents themselves should be written to the output folder (in the `--broken-` and `--invalid` files). Note that this concerns the *actual documents*; the parse or validation errors they generated are always logged in the error file. Default is _false_.

**`setIncludeDataWithErrors($state)`**
Method to set whether the actual offending data of documents that didn't validate should be added to the error file, in addition to the error meta-data. Default is _false_. Setting to _true_ will give you potentially enormous error files. Whenever possible, the error meta-data contains everything needed to track errors, so best leave this set to _false_ unless maybe your input files are so big that they're hard to handle.

**`setSilent($state)`**
For suppressing unrequested feedback to stdout. You can always use `getSettingsOverview()` to get a summary of all the active settings, and `getValidationOverview()` to get feedback on the validation results (after you've ran a validation by calling `run()`).

**`setFailOnAnyError($state)`**
Method to control whether the entire validation process aborts on the first error (either parse or validation). Default is _false_. Useful if you want to make sure that only complete datasets get sent through to the next processing step.

**`setUseISO8601DateCheck($state)`**
Swicth to using ISO8601 rather than RFC3339 as validation scheme for date-time variables. ISO8601 is more tolerant, so be aware that RFC3339 is the JSON Schema-standard. Using the JSON-schema's directly will always cause date-times to be verified against RFC3339.

**`addFileToValidate($file)`**
Add a file to validate.


### Constants
The class contains the following constants:
- `LINES_PER_OUTPUT_FILE` =_500000_: max lines of all output files.
- `DELETE_OUTPUT_ON_FATAL_EXCEPTION` =_true_: whether to delete all output when a fatal exception occurs.
- `COMMENT_CHARS`  = _['#','//']_: trimmed lines that start with one of these characters are ignored (as are empty ones).


### Using 'create_dataset.php' and 'validate.php' (to be expanded)
```
php create_dataset.php --config=example.ini --repository=/path/to/datasets/
```
Creates a dataset from all JSON(L)-files in the input directories in the specified ini-file.

```
php validate.php --repository=/path/to/datasets/ --outdir=/path/to/output/
```
Reads datasets with status 'pending' from the dataset repository, and processes its contents.

