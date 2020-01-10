<?php

/**
 * NBA JSON validator
 *
 * @author     Maarten Schermer <maarten.schermer@naturalis.nl>
 * @license    GNU General Public License v2.0
 * @version    v1.5 (October 2018)
 */

class JsonValidator {

	private $output_dir;
	private $schema_file;
	private $additional_schema_file;
	private $schema_file_original;
	private $schema;

	private $save_file;
	private $save_file_invalid;
	private $save_file_broken;
	private $error_file;

	private $save_file_counter=0;
	private $save_file_broken_counter=0;
	private $save_file_invalid_counter=0;
	private $error_file_counter=0;

	private $json_docs=[];
	private $invalid_docs=[];
	private $broken_docs=[];
	private $errors=[];
	private $element_path_error_summary=[];

	private $total_lines=0;
	private $lines_processed=0;
	private $lines_read=0;

	private $total_files_read=0;
	private $total_lines_read=0;
	private $total_valid_docs=0;
	private $total_broken_docs=0;
	private $total_invalid_docs=0;
	private $total_errors=0;
	private $all_ids=[];
	private $cycles=0;

	private $silent=false;
	private $source_system_defaults=false;
	private $file_list=[];
	private $load_error_threshold=0; # if all first $load_error_threshold lines fail = abort
	private $write_broken_and_invalid=false;
	private $include_data_with_errors=false;
	private $allow_double_ids=true;
	private $export_ids=false;
	private $sqlite_path=""; // leave empty for in-memory database
	private $fail_on_any_error=false;
	private $report_on_empty_lines=false;
	private $read_buffer_size=500000; # decrease when you run out of memory
	private $id_element_name="id";
	private $source_system_fields=["source_system_code","source_system_name","source_id","source_institution_id"];
	private $db = null;
	private $source_system_field_mappings=[];
	private $use_ISO8601_date_check=false;
	private $max_outfile_length=0;
	private $documentType;

	const LINES_PER_SYSTEM_OUT_FILE = 500000;
	const DELETE_OUTPUT_ON_FATAL_EXCEPTION = true;
	const COMMENT_CHARS = ['#','//'];

	public function __construct( $p )
	{
		if (isset($p['output_dir']))
		{
			$this->output_dir = realpath($p['output_dir'])."/";
		}
		else
		{
			throw new Exception("No output dir specified");
		}

		if (isset($p['schema_file']))
		{
			$this->schema_file = $p['schema_file'];
			$this->_readAndVerifyJsonSchema();
		}
		else
		{
			throw new Exception("No schema file specified");
		}

		foreach ([$this->output_dir] as $dir)
		{
		 	if (!file_exists($dir))
		 	{
		 		throw new Exception(sprintf('Directory "%s" doesn\'t exist',$dir));
		 	}
		}

		if (isset($p['job_id']))
		{
			$this->job_id = $p['job_id'];
		} 
		else
		{
			$this->job_id = strftime("%Y%m%d-%H%M%S");

			if (isset($p['data_supplier']))
			{
				$this->job_id = $p['data_supplier'] . '-' . $this->job_id;
			}
		}

		if (isset($p['data_type']))
		{
			$this->job_id = $this->job_id . '-' . $p['data_type'];
		} 

        $this->save_file = $this->job_id . '--%03s.jsonl';
        $this->save_file_broken = $this->job_id . '--broken--%03s.txt';
        $this->save_file_invalid = $this->job_id . '--invalid--%03s.jsonl';
        $this->error_file = $this->job_id . '--errors--%03s.jsonl';
        $this->non_unique_id_sample_file = $this->job_id . '--non_unique_ids_sample.csv';
        $this->error_summary_file = $this->job_id . '--error-summary.json';
        $this->export_ids_file_tpl = $this->job_id . '--ids-%s.csv';

        $this->sqlite_path = sprintf($this->sqlite_path,$job_id);
        $this->max_outfile_length = self::LINES_PER_SYSTEM_OUT_FILE;
	}

	public function __destruct()
	{
		if ($this->db) $this->db->close();
		if (isset($this->schema_file_original)) unlink($this->schema_file);
	}

	public function setSourceSystemDefaults( $p )
	{
		/*
			set defaults values for 
				sourceSystem.code, sourceSystem.name, sourceID, sourceInstitutionID
			with which to overwrite the values for these fields in the supplied documents (enforced defaults)
		*/
		if (!is_array($p) && $p!==false)
		{
			return;
		}

		try
		{
			foreach ($this->source_system_fields as $field)
			{
				if (isset($p[$field]))
				{
					$this->source_system_defaults[$field] = trim($p[$field]);
				}
			}
			$this->_checkSourceSystemDefaults();
		}
		catch(Exception $e)
		{
			$this->_feedback(sprintf('ERROR: %s',$e->getMessage()), true);
			exit(1);
		}
	}

	public function setIdElementName($name)
	{
		if (is_string($name) && strlen($name)>0)
		{
			$this->id_element_name = $name;
		}
	}

	public function setReadBufferSize($read_buffer_size)
	{
		if (is_numeric($read_buffer_size) && $read_buffer_size>=10)
		{
			$this->read_buffer_size = intval($read_buffer_size);
		}
	}

	public function setLoadErrorThreshold($threshold)
	{
		if (is_numeric($threshold) && $threshold>=0)
		{
			$this->load_error_threshold = $threshold;
		}
	}

	public function setWriteBrokenAndInvalid($state)
	{
		if (is_bool($state))
		{
			$this->write_broken_and_invalid = $state;
		}
	}

	public function setIncludeDataWithErrors($state)
	{
		if (is_bool($state))
		{
			$this->include_data_with_errors = $state;
		}
	}

	public function setAllowDoubleIds($state)
	{
		if (is_bool($state))
		{
			$this->allow_double_ids = $state;
		}
	}

	public function setExportIds($state)
	{
		if (is_bool($state))
		{
			$this->export_ids = $state;
		}
	}

	public function setSQLitePath($path)
	{
		$this->sqlite_path = $path;
	}

	public function setMaxOutfileLength($max_outfile_length)
	{
		if (is_numeric($max_outfile_length) && ($max_outfile_length==0 || $max_outfile_length>=1000))
		{
			$this->max_outfile_length = intval($max_outfile_length);
		}
	}

	public function setFailOnAnyError($state)
	{
		if (is_bool($state))
		{
			$this->fail_on_any_error = $state;
		}
	}

	public function setUseISO8601DateCheck($state)
	{
		if (is_bool($state))
		{
			$this->use_ISO8601_date_check = $state;
		}
	}

	public function setSilent($state)
	{
		if (is_bool($state))
		{
			$this->silent = $state;
		}
	}

	public function addFileToValidate( $file, $original_name=null )
	{
		if (file_exists( $file ))
		{
			$this->file_list[]=[ "file"=>$file, "original_name"=>$original_name ];
		} 
		else
		{
			throw new Exception(sprintf("File %s doesn't exist",$file));
		}
	}

	public function setAdditionalJsonSchema( $file )
	{
		if (file_exists( $file ))
		{
			$this->additional_schema_file=$file;
			$this->_checkAdditionalJsonSchema();
			$this->_mergeJsonSchemas();
		} 
		else
		{
			throw new Exception(sprintf("File %s doesn't exist",$file));
		}
	}

	public function setDocumentType( $type )
	{
		// $this->documentType for logging purposes only
		$this->documentType = $type;
	}

	public function getErrorSummary( $encode=false )
	{
		return $encode ? 
			json_encode($this->element_path_error_summary) :
			$this->element_path_error_summary;
	}

	public function getDoubleIdFilePath()
	{
		return isset($this->output_files['non_unique_ids']) ? $this->output_files['non_unique_ids'] : null;
	}

	public function run()
	{
		$this->_doSourceSystemDefaults();
		$this->_initializeSQLite();
		$this->_setTotalLines();

		$this->_feedback(sprintf("lines: %s",$this->total_lines));

		foreach ($this->file_list as $key => $file_item)
		{
			$filename = $file_item["file"];
			$filename_for_reports = is_null($file_item["original_name"]) ? basename($file_item["file"]) : basename($file_item["original_name"]);

			if ($file = fopen($filename, "r"))
			{
				$this->_feedback(sprintf("reading: %s",$filename));

				$line=1;
			    while(!feof($file))
			    {
			    	// get a line from the current input file
			        $doc=trim(fgets($file));

			        // set it to "" when it's a comment
			        $doc=$this->_nullifyCommentLine($doc);

			        // if it's an empty line, report on it (if we want) and continue to the next line
			        if (strlen($doc)==0)
			        {
			        	if ($this->report_on_empty_lines)
			        	{
							$this->_addError([
								'error_type'=>'document error', 
								'error_message'=>'document is empty',
								'file'=>basename($filename_for_reports),
								'line'=>$line]);
						}
						continue;
			        }

			        // parse the line to see if it is valid JSON (if not, an error is generated in the function)
			        $res=$this->_parseDocument($doc,$line,$filename_for_reports);

			        // add the valid JSON documents to the stack of documents to be validated
			        if ($res!==false)
			        {
			        	$this->json_docs[]=['file'=>$filename_for_reports,'line'=>$line,'doc'=>$res];
			        }
			        else
			        {
			        	$this->broken_docs[]=$doc;
			        }

			        // bookkeeping
			        $line++;
			        $this->lines_read++;
			        $this->total_lines_read++;

			        // if *all* lines are broken, something might be fundamentally wrong, so we abort
			        if (
			        	$this->load_error_threshold>0 && 
			        	$this->lines_read==$this->load_error_threshold && 
			        	count($this->broken_docs)==$this->lines_read
			        )
			        {
			        	throw new Exception(sprintf('First %s lines of "%s" are broken (tripped load_error_threshold)',
			        		$this->lines_read,$filename_for_reports));
			        }

			        // once we have enough valid JSON documents, we start validating ('enough' can be manipulated to accomodate memory restrictions)
			        if ($this->lines_read>=$this->read_buffer_size)
			        {
			        	/*
			        		take care:
			        		going into _validateJsonDocuments(), $this->json_docs is an array of:
			        			['file'=>$filename,'line'=>$line,'doc'=>$valid_doc];
			        		but afterwards, it is an array of just valid JSON-docs.
			        	*/
			        	$this->_validateJsonDocuments();
			        	$this->_checkForDoubleIDs();
						$this->_writeFiles();
						$this->_writeErrors();
						$this->_giveCycleFeedback();

						$this->lines_read=0;
						$this->json_docs=[];
						$this->broken_docs=[];
						$this->invalid_docs=[];
						$this->errors=[];
						$this->cycles++;
			        }
			    }
			    fclose($file);
			    $this->total_files_read++;
			}
			else
			{
				throw new Exception(sprintf('Cannot open %s for reading',$filename));
			}
		}

		// processing leftovers
    	$this->_validateJsonDocuments();
		$this->_checkForDoubleIDs();
		$this->_writeFiles();
		$this->_writeErrors();

		// export ID's (if we've stored any)
		$this->_exportIds();
    }

    public function getErrors($include_data=false)
    {
    	if ($include_data)
    	{
    		return $this->errors;
    	}
    	else
    	{
        	return array_map(function($x) { unset($x['data']); return $x; },$this->errors);
    	}
    }

    public function getJsonDocuments()
    {
        return $this->json_docs;
    }

    public function getJsonDocumentCount()
    {
        return count($this->json_docs);
    }

    public function getInvalidJsonDocuments()
    {
        return $this->invalid_docs;
    }

    public function getSettingsOverview()
    {
		if (isset($this->schema_file_original))
		{
	    	$d = [
				'schema_file' => $this->schema_file_original,
				'additional_schema_file' => $this->additional_schema_file,
			];
		}
		else
		{
	    	$d = [
				'schema_file' => $this->schema_file,
			];
		}

    	$d += [
			'input_files' => $this->file_list,
			'output_dir' => $this->output_dir,
			'save_file' => $this->save_file,
			'save_file_invalid' => $this->save_file_invalid,
			'save_file_broken' => $this->save_file_broken,
			'error_file' => $this->error_file,
			'report_on_empty_lines' => $this->report_on_empty_lines,
			'load_error_threshold' => $this->load_error_threshold,
			'id_element_name' => $this->id_element_name,
			'allow_double_ids' => $this->allow_double_ids,
			'export_ids' => $this->export_ids,
			'fail_on_any_error' => $this->fail_on_any_error,
			'read_buffer_size' => $this->read_buffer_size,
			'use_ISO8601_date_check' => $this->use_ISO8601_date_check,
			'write_broken_and_invalid' => $this->write_broken_and_invalid,
			'included_data_with_errors' => $this->include_data_with_errors,
		];

		if ($this->source_system_defaults===false)
		{
			$d += [ 'source_system_defaults' => '(keep originals)' ];
		}
		else
		{
			foreach ($this->source_system_fields as $field)
			{
				if (!isset($this->source_system_defaults[$field]))
				{
					$d += [ 'source_system_defaults:' . $field => '(keep original)' ];
				} 
				else {
					$d += [ 'source_system_defaults:' . $field => $this->source_system_defaults[$field] ];
				}
			}
		}

    	$d += [
			'LINES_PER_SYSTEM_OUT_FILE' => self::LINES_PER_SYSTEM_OUT_FILE,
			'DELETE_OUTPUT_ON_FATAL_EXCEPTION' => self::DELETE_OUTPUT_ON_FATAL_EXCEPTION,
		];

		return $d;
    }

    public function getValidationOverview()
    {
    	return [
    		'files_read' => $this->total_files_read,
			'lines_read' => $this->total_lines_read,
			'valid_json_docs' => $this->total_valid_docs,
			'broken_docs' => $this->total_broken_docs,
			'invalid_json_docs' => $this->total_invalid_docs,
			'errors' => $this->total_errors,
			'ids_total' => $this->allow_double_ids ? 'n/a' : $this->_getIdCount(),
			'ids_unique' => $this->allow_double_ids ? 'n/a' : $this->_getIdCount("unique"),
			'infiles' => $this->file_list,
			'outfiles' => [
				'valid' => isset($this->output_files['valid']) ? array_unique($this->output_files['valid']) : null,
				'errors' => isset($this->output_files['errors']) ? array_unique($this->output_files['errors']) : null,
				'invalid' => isset($this->output_files['invalid']) ? array_unique($this->output_files['invalid']) : null,
				'broken' => isset($this->output_files['broken']) ? array_unique($this->output_files['broken']) : null,
				'id\'s' => $this->export_ids ? $this->output_files['ids'] : 'n/a',
				'non-unique id\'s' => isset($this->output_files['non_unique_ids']) ? $this->output_files['non_unique_ids'] : 'n/a',
			]
		];
    }

    public function validateRawJsonDoc($raw_json,$line=0,$filename="stdin")
    {
    	// $line and $filename are for error logging purposes only

		$this->json_docs=[];
		$this->invalid_docs=[];
		$this->broken_docs=[];
		$this->errors=[];
		
		$doc = trim($raw_json);

		$res=$this->_parseDocument($doc,$line,$filename);

		if ($res!==false)
		{
	    	$this->json_docs[]=['file'=>$filename,'line'=>$line,'doc'=>$res];
	    }
	    else
	    {
	    	$this->broken_docs[]=$doc;
	    }

		$this->_validateJsonDocuments();

		print(sprintf("error(s): %s\n",count($this->errors)));

		$value_cutoff = 100;

		foreach($this->errors as $key=>$error)
		{
			$causes=[];

			foreach ($error["cause"] as $ekey=>$cause)
			{
				if (is_array($cause))
				{
					$causes[$ekey]=json_encode($cause);
				}
				else
				{
					$causes[$ekey]=$cause;
				}
				if ($value_cutoff!=0 && strlen($causes[$ekey])>$value_cutoff)
				{
					$causes[$ekey]=substr($causes[$ekey],0,$value_cutoff) . "... (value truncated)" ;
				}
			}

			echo sprintf("%s. %s: %s\n   cause(s): %s\n   path: %s\n",
				$key,
				$error["error_type"],
	            $error["error_message"],
	            implode("; ",$causes),
	            // implode("; ",$error["cause"]),
	            $error["element_path"]		            
	         );
		}
    }

	public function runFileListValidation()
	{
		$this->_doSourceSystemDefaults();
		$this->_initializeSQLite();
		$this->_setTotalLines();

		$this->_feedback(sprintf("lines: %s",$this->total_lines));

		foreach ($this->file_list as $key => $file_item)
		{
			$filename = $file_item["file"];
			$filename_for_reports = is_null($file_item["original_name"]) ? basename($file_item["file"]) : basename($file_item["original_name"]);

			if ($file = fopen($filename, "r"))
			{
				$this->_feedback(sprintf("reading: %s",$filename));

				$this->current_line=1;

				while(!feof($file))
				{
					$doc=trim(fgets($file));

					if (!$this->_addToLineBufferIfValidJson( $doc, $filename_for_reports ))
					{
						continue;
					}

					$this->_validateLineBuffer();
				}

				fclose($file);

				$this->total_files_read++;
			}
			else
			{
				throw new Exception(sprintf('Cannot open %s for reading',$filename));
			}
		}

		// processing leftovers
    	$this->_validateLineBuffer( true );

		// export ID's (if we've stored any)
		$this->_exportIds();
    }

	public function addDocToValidate( $doc, $line_number, $filename_for_reports )
	{
		$doc=trim($doc);

		$this->current_line = $line_number;
		$this->line_fed_documents[]=$filename_for_reports;
		$this->total_files_read = count(array_unique($this->line_fed_documents));
		$this->_addToLineBufferIfValidJson( $doc, $filename_for_reports );
	}

	public function setTotalDocListLength( $num )
	{
		if (is_int($num))
		{
			$this->total_lines = $num;
		}
	}

	public function runDocListValidation( $finalize=false )
	{
		$this->_doSourceSystemDefaults();
		$this->_initializeSQLite();
		$this->_validateLineBuffer( $finalize );
		if ($finalize)
		{
			// export ID's (if we've stored any)
			$this->_exportIds();
		}
	}

	private function _addToLineBufferIfValidJson( $doc, $filename_for_reports )
	{
		// set it to "" when it's a comment
		$doc=$this->_nullifyCommentLine($doc);

		// if it's an empty line, report on it (if we want) and continue to the next line
		if (strlen($doc)==0)
		{
			if ($this->report_on_empty_lines)
			{
				$this->_addError([
					'error_type'=>'document error', 
					'error_message'=>'document is empty',
					'file'=>basename($filename_for_reports),
					'line'=>$this->current_line]);
			}
			return false;
		}

		// parse the line to see if it is valid JSON (if not, an error is generated in the function)
		$res=$this->_parseDocument($doc,$this->current_line,$filename_for_reports);

		// add the valid JSON documents to the stack of documents to be validated
		if ($res!==false)
		{
			$this->json_docs[]=['file'=>$filename_for_reports,'line'=>$this->current_line,'doc'=>$res];
		}
		else
		{
			$this->broken_docs[]=$doc;
		}

		// bookkeeping
		$this->current_line++;
		$this->lines_read++;
		$this->total_lines_read++;

		// if *all* lines are broken, something might be fundamentally wrong, so we abort
		if (
			$this->load_error_threshold>0 && 
			$this->lines_read==$this->load_error_threshold && 
			count($this->broken_docs)==$this->lines_read
		)
		{
			throw new Exception(sprintf('First %s lines of "%s" are broken (tripped load_error_threshold)',
				$this->lines_read,$filename_for_reports));
		}
	}

	private function _validateLineBuffer( $run_regardless_of_buffer_size=false )
	{
	    // once we have enough valid JSON documents, we start validating ('enough' can be manipulated to accomodate memory restrictions)
	    if (($this->lines_read>=$this->read_buffer_size) || $run_regardless_of_buffer_size)
		{
			/*
				take care:
				going into _validateJsonDocuments(), $this->json_docs is an array of:
	    			['file'=>$filename,'line'=>$line,'doc'=>$valid_doc];
	    		but afterwards, it is an array of just valid JSON-docs.

				$run_regardless_of_buffer_size is for processing the leftovers
	    	*/

	    	$this->_validateJsonDocuments();
	    	$this->_checkForDoubleIDs();
			$this->_writeFiles();
			$this->_writeErrors();
			$this->_giveCycleFeedback();

			$this->lines_read=0;
			$this->json_docs=[];
			$this->broken_docs=[];
			$this->invalid_docs=[];
			$this->errors=[];
			$this->cycles++;
	    }
	}

	private function _getNumberOfFileLines($filename)
	{
		return intval(exec(sprintf("cat %s | wc -l",escapeshellarg($filename))));
	}

	private function _setTotalLines()
	{
		foreach ($this->file_list as $key => $file_item)
		{
			$this->total_lines += $this->_getNumberOfFileLines($file_item["file"]);
		}		
	}

	private function _readAndVerifyJsonSchema()
	{
		$this->schema = json_decode(file_get_contents($this->schema_file));
		if (is_null($this->schema))
		{
			throw new Exception(sprintf('Failed to parse schema file "%s"',$this->schema_file));
		}
	}

	private function _setSourceSystemFieldMappings()
	{
		if (!$this->schema->properties->sourceSystem->properties->code->enum)
		{
			throw new Exception("Schema misses core field enum: sourceSystem->code->enum");
		}
		if (!$this->schema->properties->sourceSystem->properties->name->enum)
		{
			throw new Exception("Schema misses core field enum: sourceSystem->name->enum");
		}
		// not present in taxon
		// if (!$this->schema->properties->sourceID->enum)
		// 	throw new Exception("Schema misses core field enum: sourceID->enum");
		// if (!$this->schema->properties->sourceInstitutionID->enum)
		// 	throw new Exception("Schema misses core field enum: sourceInstitutionID->enum");

		$this->source_system_field_mappings = [
			'source_system_code' => $this->schema->properties->sourceSystem->properties->code->enum,
			'source_system_name' => $this->schema->properties->sourceSystem->properties->name->enum,
			'source_id' => isset($this->schema->properties->sourceID->enum) ? $this->schema->properties->sourceID->enum : "",
			'source_institution_id' => isset($this->schema->properties->sourceInstitutionID->enum) ? $this->schema->properties->sourceInstitutionID->enum : ""
		];

	}

	private function _checkAdditionalJsonSchema()
	{
		if (is_null($this->additional_schema_file))
		{
			return;
		}
		$tmp = json_decode(file_get_contents($this->additional_schema_file));
		if (is_null($tmp))
		{
			throw new Exception(sprintf('Failed to parse schema file "%s"',$this->additional_schema_file));
		}
	}

	private function _mergeJsonSchemas()
	{
		if (is_null($this->additional_schema_file))
		{
			return;
		}

		$tmpSchema = tempnam(sys_get_temp_dir(), "nba");
		$handle = fopen($tmpSchema, "w");

		# do not change order of main and additional files!
		fwrite($handle, shell_exec("jq -s '.[0] * .[1]' " . $this->schema_file. " " . $this->additional_schema_file));
		fclose($handle);

		$this->schema_file_original = $this->schema_file;
		$this->schema_file = $tmpSchema; 
		$this->schema = json_decode(file_get_contents($this->schema_file));

		if (is_null($this->schema))
		{
			throw new Exception(sprintf('Failed to parse schema file "%s"',$this->schema_file));
		}
	}

	private function _parseDocument($doc,$line,$file)
	{
		$res=json_decode($doc);
		if (is_null($res))
		{
			if ($this->fail_on_any_error)
			{
	        	throw new Exception(sprintf("invalid JSON (file: %s, line %s, error: %s); tripped over fail_on_any_error",
	        		basename($file),$line,$this->_getLastJsonError()));
			}

			$this->_addError([
				'error_type'=>'invalid JSON', 
				'error_message'=>$this->_getLastJsonError(),
				'file'=>basename($file),
				'line'=>$line,
				'data'=>$doc]);
			return false;
		}
		else
		{
			return $res;
		}			
    }

	private function _validateJsonDocuments()
	{

		$this->_feedback("validating");

		$consistent=[];
		
		$i=0;

		foreach($this->json_docs as $object)
		{
			$doc=$object['doc'];

			$validator = new League\JsonGuard\Validator($doc, $this->schema);

			$validator->getRuleset()->get('format')->addExtension('geo-json', new GeoJsonExtension());

			if ($this->use_ISO8601_date_check)
			{
				$validator->getRuleset()->get('format')->addExtension('date-time', new ISO8601DateTimeFormatExtension());
			}

			if ($validator->passes())
			{
				$this->total_valid_docs++;

				if ($this->source_system_defaults!==false)
				{
					if (isset($this->source_system_defaults['source_system_code']) &&
						!is_null($this->source_system_defaults['source_system_code']))
		                $doc->sourceSystem->code = $this->source_system_defaults['source_system_code'];

					if (isset($this->source_system_defaults['source_system_name']) &&
						!is_null($this->source_system_defaults['source_system_name']))
		                $doc->sourceSystem->name = $this->source_system_defaults['source_system_name'];
					
					if (isset($this->source_system_defaults['source_id']) &&
						!is_null($this->source_system_defaults['source_id']))
		                $doc->sourceID = $this->source_system_defaults['source_id'];
					
					if (isset($this->source_system_defaults['source_institution_id']) &&
						!is_null($this->source_system_defaults['source_institution_id']))
		                $doc->sourceInstitutionID = $this->source_system_defaults['source_institution_id'];
	            }

				$consistent[]=$doc;

                if (isset($doc->{$this->id_element_name}))
                {
					if (!$this->allow_double_ids || $this->export_ids)
					{
						$this->_storeId($doc->{$this->id_element_name});
					}
                }
			}
			else
			{
                if (isset($doc->{$this->id_element_name}))
                {
                    $id = $doc->{$this->id_element_name};
                }
                else
                {
                    $id = "?";
                }

				foreach ($validator->errors() as $error)
				{
					if ($this->fail_on_any_error)
					{
						throw new Exception(sprintf("validation error (file: %s, line %s, id: %s, error: %s); tripped over fail_on_any_error",
							basename($object['file']),$object['line'],$id,$error->getMessage()));
					}

					$errorToAdd = [
						'error_type'=>'validation error', 
						'error_message'=>$error->getMessage(),
						'element_path'=>$error->getDataPath(),
						'cause'=>$error->getCause(),
						'id'=>$id,
						'data'=>$doc,
						'file'=>basename($object['file']),
						'line'=>$object['line'] 
					];

					$this->_addError($errorToAdd);
					$this->_addElementPathErrorSummary($errorToAdd);
				}

				$this->invalid_docs[]=$doc;

		        if (
		        	$this->load_error_threshold>0 && 
		        	count($this->invalid_docs)==$this->load_error_threshold && 
		        	count($consistent)==0
		        )
		        {
		        	$this->_writeErrors();
		        	throw new Exception(sprintf("First %s lines didn't validate (tripped load_error_threshold); see %s for errors.",
		        		$this->load_error_threshold, sprintf($this->error_file,$this->error_file_counter)));
		        }
			}

			$i++;
		}

		$this->json_docs=$consistent;
    }

	private function _writeFiles()
	{
		$this->_feedback("writing valid");

		// all in one file
		if ($this->max_outfile_length==0)
		{
			$save_file=$this->output_dir . sprintf($this->save_file,$this->save_file_counter);
			file_put_contents($save_file,implode(PHP_EOL,array_map('json_encode', $this->json_docs)).PHP_EOL,FILE_APPEND);
			$this->output_files['valid'][]=$save_file;
		}
		// split files
		else
		{
			while(count($this->json_docs)>0)
			{
				$save_file=$this->output_dir . sprintf($this->save_file,$this->save_file_counter);
				$buffer=file_exists($save_file) ? $this->_getNumberOfFileLines($save_file) : 0;

				if ($buffer>=$this->max_outfile_length)
				{
					$this->save_file_counter++;
					$save_file=$this->output_dir . sprintf($this->save_file,$this->save_file_counter);
					$buffer = 0;
				}

				$chunk=array_splice($this->json_docs, 0, self::LINES_PER_SYSTEM_OUT_FILE-$buffer);
				file_put_contents($save_file,implode(PHP_EOL,array_map('json_encode', $chunk)).PHP_EOL,FILE_APPEND);
				$this->output_files['valid'][]=$save_file;
			}
		}
	}

	private function _writeErrors()
	{
		$this->_feedback("writing errors");

		$this->total_broken_docs+=count($this->broken_docs);
		$this->total_invalid_docs+=count($this->invalid_docs);
		$this->total_errors+=count($this->errors);

		while(count($this->errors)>0)
		{
			$save_file=$this->output_dir . sprintf($this->error_file,$this->error_file_counter);
			$buffer=file_exists($save_file) ? $this->_getNumberOfFileLines($save_file) : 0;

			if ($buffer>=self::LINES_PER_SYSTEM_OUT_FILE)
			{
				$this->error_file_counter++;
				$save_file=$this->output_dir . sprintf($this->error_file,$this->error_file_counter);
				$buffer = 0;
			}

			$chunk=array_splice($this->errors, 0, self::LINES_PER_SYSTEM_OUT_FILE-$buffer);

			if ($this->include_data_with_errors)
			{
				file_put_contents($save_file,implode(PHP_EOL,array_map('json_encode', $chunk)).PHP_EOL,FILE_APPEND);
			}
			else
			{
				file_put_contents($save_file,implode(PHP_EOL,array_map('json_encode', array_map(function($x) { unset($x['data']); return $x; },$chunk))).PHP_EOL,FILE_APPEND);
			}

			$this->output_files['errors'][]=$save_file;
		}


		if (count($this->element_path_error_summary)>0)
		{
			$save_file=$this->output_dir . $this->error_summary_file;
			file_put_contents($save_file,json_encode($this->element_path_error_summary));
			$this->output_files['errors'][]=$save_file;
		}


		if ($this->write_broken_and_invalid)
		{
			while(count($this->broken_docs)>0)
			{
				$save_file=$this->output_dir . sprintf($this->save_file_broken,$this->save_file_broken_counter);
				$buffer=file_exists($save_file) ? $this->_getNumberOfFileLines($save_file) : 0;

				if ($buffer>=self::LINES_PER_SYSTEM_OUT_FILE)
				{
					$this->save_file_broken_counter++;
					$save_file=$this->output_dir . sprintf($this->save_file_broken,$this->save_file_broken_counter);
					$buffer = 0;
				}

				$chunk=array_splice($this->broken_docs, 0, self::LINES_PER_SYSTEM_OUT_FILE-$buffer);
				file_put_contents($save_file,implode(PHP_EOL,$chunk).PHP_EOL,FILE_APPEND);
				$this->output_files['broken'][]=$save_file;
			}

			while(count($this->invalid_docs)>0)
			{
				$save_file=$this->output_dir . sprintf($this->save_file_invalid,$this->save_file_invalid_counter);
				$buffer=file_exists($save_file) ? $this->_getNumberOfFileLines($save_file) : 0;

				if ($buffer>=self::LINES_PER_SYSTEM_OUT_FILE)
				{
					$this->save_file_invalid_counter++;
					$save_file=$this->output_dir . sprintf($this->save_file_invalid,$this->save_file_invalid_counter);
					$buffer = 0;
				}

				$chunk=array_splice($this->invalid_docs, 0, self::LINES_PER_SYSTEM_OUT_FILE-$buffer);
				file_put_contents($save_file,implode(PHP_EOL,array_map('json_encode', $chunk)).PHP_EOL,FILE_APPEND);			
				$this->output_files['invalid'][]=$save_file;
			}
		}
    }

    private function _exportIds()
    {
		if ($this->export_ids==false) return;

		$this->_feedback("writing id's");

		$stmts = [
			"all" => "select doc_id from all_ids",
			"unique" => "select doc_id from unique_ids",
			"multiple" => "select doc_id, count(*) as c from all_ids group by doc_id having count(*) > 1"
		];

		foreach ($stmts as $key=>$stmt)
		{
			$sql = $this->db->prepare($stmt);
			$query = $sql->execute();
			$save_file=$this->output_dir . sprintf($this->export_ids_file_tpl,$key);

			$export_buffer=[];
			while ($row = $query->fetchArray())
			{
				if(isset($row["c"]))
				{
					$export_buffer[]=$row["doc_id"] . "\t" . $row["c"];
				}
				else
				{
					$export_buffer[]=$row["doc_id"];
				}

				if (count($export_buffer)>=10000)
				{
					file_put_contents($save_file,implode(PHP_EOL,$export_buffer).PHP_EOL,FILE_APPEND);
					$export_buffer=[];
				}
			}

			if (count($export_buffer)>0)
			{
				file_put_contents($save_file,implode(PHP_EOL,$export_buffer).PHP_EOL,FILE_APPEND);
			}

			if (file_exists($save_file))
			{
				$this->output_files['ids'][]=$save_file;
			}

			$this->_feedback(sprintf("wrote non-unqiue ID's to %s",$save_file));
		}

    }

	private function _checkSourceSystemDefaults()
	{
		if ($this->source_system_defaults==false)  return;

		foreach ($this->source_system_fields as $field)
		{
			if (!isset($this->source_system_defaults[$field]))
			{
				continue;
			}
			if (!is_null($this->source_system_defaults[$field]) && empty($this->source_system_defaults[$field]))
			{
				throw new Exception("Empty string specified for source system default: " . $field);
			}
		}
	}

	private function _validateSystemDefaults()
	{
		if ($this->source_system_defaults==false) return;

		foreach ($this->source_system_fields as $field)
		{
			if (!isset($this->source_system_defaults[$field]))
			{
				continue;
			}

			if (isset($this->source_system_field_mappings[$field]))
			{
				$valid_values = $this->source_system_field_mappings[$field];
				$supplied_value = $this->source_system_defaults[$field];

				if (!in_array($supplied_value, $valid_values))
				{
					throw new Exception(sprintf('Supplied value "%s" for "%s" is invalid (possible values: %s)',
						$supplied_value,$field,implode("; ",$valid_values)));
				}
			}
			else
			{
				throw new Exception(sprintf('There is no schema mapping for "%s"',$field));
			}
		}
	}

	private function _doSourceSystemDefaults()
	{
		$this->_setSourceSystemFieldMappings();
		$this->_checkSourceSystemDefaults();
		$this->_validateSystemDefaults();
	}

    private function _addError($error)
    {
        $this->errors[] = $error;
    }

    private function _addElementPathErrorSummary($error)
    {
    	// array elements have the index number as part of their path
		$element_path = $error['element_path'];
		$boom = explode("/",$element_path);
		if (is_numeric(end($boom)))
		{
			reset($boom);
			array_pop($boom);
			$element_path = implode("/",$boom);
		}

		$index = substr(sha1($element_path.':'.$error['error_type']),0,8);

    	if (!isset($this->element_path_error_summary[$index]))
    	{
    		$c = is_array($error['cause']) || is_object($error['cause']) ? json_encode($error['cause']) : $error['cause'];
    		$l = strlen($c);
			$cause = $l > 250 ? sprintf("%s ... (truncated from %s characters)",substr($c, 0, 250),$l) : $c;

    		$example = [
				'error_type' => $error['error_type'],
				'error_message' => $error['error_message'],
				'element_path' => $element_path,
				'cause (first occurrence)' => $cause,
				'id (first occurrence)' => $error['id'],
				'file (first occurrence)' => $error['file'],
				'line (first occurrence)' => $error['line'],
			];

    		$this->element_path_error_summary[$index]=['count' => 1,'example' => $example];
    	}
    	else
    	{
    		$this->element_path_error_summary[$index]['count']++;
    	}
    }

	private function _feedback($msg,$exception=false)
	{
		if (!$this->silent || $exception)
		{
			echo $msg,"\n";
		}
	}

    private function _getLastJsonError()
    {
		switch (json_last_error())
		{
			case JSON_ERROR_NONE:
				return 'no errors';
				break;
			case JSON_ERROR_DEPTH:
				return 'maximum stack depth exceeded';
				break;
			case JSON_ERROR_STATE_MISMATCH:
				return 'underflow or the modes mismatch';
				break;
			case JSON_ERROR_CTRL_CHAR:
				return 'unexpected control character found';
				break;
			case JSON_ERROR_SYNTAX:
				return 'syntax error, malformed JSON';
				break;
			case JSON_ERROR_UTF8:
				return 'malformed UTF-8 characters, possibly incorrectly encoded';
				break;
			default:
				return 'unknown error';
				break;
    	}
    }

    private function _deleteOutput()
    {
    	foreach(
    		array_merge(
    			(isset($this->output_files['valid']) ? array_unique($this->output_files['valid']) : []),
	    		(isset($this->output_files['errors']) ? array_unique($this->output_files['errors']) : []),
	    		(isset($this->output_files['invalid']) ? array_unique($this->output_files['invalid']) : []),
	    		(isset($this->output_files['broken']) ? array_unique($this->output_files['broken']) : [])
	    	) as $file) {
			unlink($file);
    	}
    }

    private function _nullifyCommentLine($doc)
    {
    	if (substr($doc,0,1) == '{')
    	{
    		return $doc;
    	}
    	foreach(self::COMMENT_CHARS as $comment_char)
    	{
			if (substr($doc,0,strlen($comment_char))==$comment_char)
			{
				return "";
			}
    	}
    	return $doc;
    }

	private function _storeId($id)
	{
		$sql = $this->db->prepare('insert into all_ids (doc_id) values (?)');
		$sql->bindValue(1, $id);
		$sql->execute();
		$sql = $this->db->prepare('insert or replace into unique_ids (doc_id) values (?)');
		$sql->bindValue(1, $id);
		$sql->execute();
	}

	private function _getIdCount($type="all")
	{
		if ($this->allow_double_ids)
		{
			return;
		}

		if ($type=="unique")
		{
			$stmt = 'select count(doc_id) from unique_ids';
		}
		else
		{
			$stmt = 'select count(doc_id) from all_ids';
		}

		$sql = $this->db->prepare($stmt);
		$query = $sql->execute();
	    $row = $query->fetchArray();
	    return $row[0];
	}

  	private function _checkForDoubleIDs()
  	{
		if ($this->allow_double_ids)
		{
			return;
		}

		$uniqueIdDifference = $this->_getIdCount() - $this->_getIdCount("unique");

		if (!$this->allow_double_ids && $this->total_valid_docs > 0 && $uniqueIdDifference > 0)
		{
			if (self::DELETE_OUTPUT_ON_FATAL_EXCEPTION)
			{
				$this->_deleteOutput();
			}

			$this->_reportNonUniqueIds();

			throw new Exception(sprintf('Found non-unique IDs (allow_double_ids = false); %s',
				self::DELETE_OUTPUT_ON_FATAL_EXCEPTION ? "deleted output" : "wrote possibly partial output"
			));
		}
	}

	private function _reportNonUniqueIds()
	{
		$stmt = 'select doc_id, count(*) as c from all_ids group by doc_id having count(*) > 1';

		$results = $this->db->query($stmt);
		$buffer=[];
		$buffer[] = "take note: this sample of the non unique ID's is not necessarily a complete list.";
		$buffer[] = "";
		while ($row = $results->fetchArray())
		{
		    $buffer[] = $row["doc_id"] . "\t" . $row["c"] . "x";
		}

		$save_file= $this->output_dir . $this->non_unique_id_sample_file;

		file_put_contents($save_file,implode(PHP_EOL,$buffer));

		$this->output_files['non_unique_ids']=$save_file;

		$this->_feedback( sprintf( "wrote a sample of non-unqiue ID's to %s",$save_file));
	}

  	private function _initializeSQLite()
  	{
		if (is_null($this->db))
		{
			$this->db = new SQLite3($this->sqlite_path, SQLITE3_OPEN_READWRITE);

			$this->db->query('CREATE TABLE IF NOT EXISTS "all_ids" (
			    "doc_id" string not null
			)');

			$this->db->query('CREATE TABLE IF NOT EXISTS "unique_ids" (
			    "doc_id" string not null unique
			)');
		}

		$sql = $this->db->prepare('delete from all_ids');
		$sql->execute();
		$sql = $this->db->prepare('delete from unique_ids');
		$sql->execute();
	}

	private function _giveCycleFeedback()
	{
		if ($this->cycles % 10 != 0) return;

		$msg = sprintf(
			"%s%% of %s lines read (%s valid, %s invalid, %s broken)",
			round(($this->total_lines_read / $this->total_lines) * 100),
			number_format($this->total_lines),
			number_format($this->total_valid_docs),
			number_format($this->total_invalid_docs),
			number_format($this->total_broken_docs)
		);

		if (!is_null($this->documentType))
		{
			$msg = $this->documentType.": ".$msg;
		}

		$this->_feedback($msg);
	}
}

use League\JsonGuard\Constraint\DraftFour\Format\FormatExtensionInterface;
use League\JsonGuard\Validator;

class ISO8601DateTimeFormatExtension implements FormatExtensionInterface
{
	private function assertISO8601DateForm($dateStr)
	{
		preg_match('/^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$/', $dateStr, $matches);
		return $matches;
	}

	private function assertDateValue($year,$month,$day)
	{
		return checkdate($month, $day, $year);
	}

    public function validate($value, Validator $validator)
    {
    	$parts=self::assertISO8601DateForm($value);

    	// format error: parts are wrong
		if (count($parts)==0)
		{
            return \League\JsonGuard\error(sprintf('Invalidly formatted date "%s"',$value), $validator);
		} 
		// date error: parts look good, but date is invalid (like feb 31)
		elseif (!self::assertDateValue($parts[1],$parts[5],$parts[7]))
		{
            return \League\JsonGuard\error(sprintf('Invalid date "%s"',$value), $validator);
		}
    }
}

class GeoJsonExtension implements FormatExtensionInterface
{
	const ACCEPTED_GEO_JSON_TYPES = ['Polygon','MultiPolygon'];

    public function validate($value, Validator $validator)
    {
		try
		{
			$geojson = \GeoJson\GeoJson::jsonUnserialize($value);

			if (!in_array($value->type,self::ACCEPTED_GEO_JSON_TYPES))
			{
				throw new Exception(sprintf('Unsupported GeoJSON-type "%s".',$value->type));
			}
		}
		catch(Exception $e)
		{
			return \League\JsonGuard\error($e->getMessage(), $validator);
		}
    }
}
