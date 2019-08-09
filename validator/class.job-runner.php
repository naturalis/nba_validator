<?php

	class jobRunner
	{
		private $job;
		private $cfg;
		private $type;
		private $outdir;
		private $validator;
		private $validatorMaxOutfileLength=500000;
		private $exportIds=false;
		private $ppdbWorksTopToBottom=true;
		private $processed_input_files=0;

		const READ_BUFFER_SIZE = 100000;
		const INDEX_FILE_FIELD_SEP = "\t";

		public function __construct( $job )
		{
			$this->_setJob($job);
		}

		public function setOutDir($outdir)
		{
			$this->outdir = $outdir;
		}

		public function setValidatorMaxOutfileLength($length)
		{
			$this->validatorMaxOutfileLength = $length;
		}

		public function setExportIds($state)
		{
			$this->exportIds = $state;
		}

		public function run()
		{
			$this->_checkConfigFile();
			$this->_readConfig();
			$this->_checkNumberOfLines();
			$this->_runValidator();
		}

		public function getJob()
		{
			return $this->job;
		}

		public function archiveOriginalFiles()
		{
			if (!isset($this->cfg["settings"]["archive_dir"]) || !file_exists($this->cfg["settings"]["archive_dir"]))
			{
				$archive_dir = sys_get_temp_dir();
			}
			else
			{
				$archive_dir = realpath($this->cfg["settings"]["archive_dir"]);
			}			

			$unlink_me=[];
			$rmdir_me=[];

			$tmpdir = sys_get_temp_dir() . "/" . uniqid();
			mkdir($tmpdir);

			foreach($this->job["input"] as $type => $files)
			{
				$tmpsubdir = $tmpdir . "/" . $type;
				if (@mkdir($tmpsubdir))
				{
					$rmdir_me[]=$tmpsubdir;
				}

				foreach($files as $file)
				{
					if (!file_exists($file["tmp_path"]))
					{
						continue;
					}
					$tmpfile = $tmpsubdir . "/" . basename($file["path"]);
					rename($file["tmp_path"],$tmpfile);
					$unlink_me[] = $tmpfile;
				}

				foreach(["errors","invalid","broken"] as $state)
				{
					if (isset($this->job["validator"]) && count((array)$this->job["validator"][$type]["results"]["outfiles"][$state])>0)
					{
						$tmpsubdir_err = $tmpsubdir . "/errors";
						mkdir($tmpsubdir_err);
						array_unshift($rmdir_me, $tmpsubdir_err);
					}
					else
					{
						continue;
					}
					foreach($this->job["validator"][$type]["results"]["outfiles"][$state] as $file)
					{
						$tmpfile = $tmpsubdir_err . "/" . basename($file);
						copy($file, $tmpfile);
						$unlink_me[] = $tmpfile;
					}
				}
			}

			foreach($this->job["delete"] as $type => $files)
			{	
				$tmpsubdir = $tmpdir . "/" . $type;
				if (@mkdir($tmpsubdir))
				{
					$rmdir_me[]=$tmpsubdir;
				}

				foreach($files as $file)
				{	
					if (!file_exists($file["tmp_path"]))
					{
						continue;
					}
					$tmpfile = $tmpsubdir . "/" . basename($file["path"]);
					copy($file["tmp_path"],$tmpfile);
					$unlink_me[] = $tmpfile;
				}
			}

			foreach($this->job["indices"] as $type => $file)
			{
				$tmpsubdir = $tmpdir . "/" . $type;
				if (@mkdir($tmpsubdir))
				{
					$rmdir_me[]=$tmpsubdir;
				}

				if ($file==false) continue;
				if (!file_exists($file["tmp_path"])) continue;
				$tmpfile = $tmpsubdir . "/" . basename($file["path"]);
				rename($file["tmp_path"],$tmpfile);
				$unlink_me[] = $tmpfile;
			}

			$tmpfile = $tmpdir . basename($this->job["dataset_filename"]);
			copy($this->job["dataset_filename"], $tmpfile);
			$unlink_me[] = $tmpfile;

			$rmdir_me[]=$tmpdir;
			$target = $archive_dir . "/" . $this->job["id"] . ".tar.gz";

			$this->_feedback(sprintf("archiving to %s",$target));

			exec("tar -zcvf " . $target . " " . $tmpdir . "/", $output);
			// print_r($output);
			foreach (array_unique($unlink_me) as $file) unlink($file);
			foreach (array_unique($rmdir_me) as $dir) rmdir($dir);

			$this->job["archived_input"]=$target;
		}

		private function _setJob( $job )
		{
			$this->job=$job;
		}

		private function _checkConfigFile()
		{
			if (!isset($this->job["supplier_config_file"]))
			{
				throw new Exception("no config file in dataset");
			} 
			else
			if (!file_exists($this->job["supplier_config_file"]))
			{
				throw new Exception(sprintf("non-existent config file %s",$this->job["supplier_config_file"]));
			} 
			else
			if (parse_ini_file($this->job["supplier_config_file"])==false)
			{
				throw new Exception(sprintf("invalid config file %s",$this->job["supplier_config_file"]));
			} 
		}

		private function _readConfig()
		{
			$this->cfg = parse_ini_file($this->job["supplier_config_file"],true,INI_SCANNER_TYPED);
		}
		
		private function _checkNumberOfLines()
		{

			$warnings=[];

			foreach($this->job["input"] as $type => $files)
			{
				$this->_feedback(sprintf("checking lines: %s",$type));
				$this->type = $type;

				foreach($files as $file)
				{
					if (isset($file["lines"]) && $file["lines"]!==false)
					{
						$actual_lines = intval(exec("cat " . $file["tmp_path"] . " | wc -l"));
						if ($actual_lines!==$file["lines"])
						{
							$warnings[$this->type][]=sprintf("number of lines mismatch in %s: index lists %s, file has %s",basename($file["path"]),$file["lines"],$actual_lines);
						}
					}
					else
					{
						$warnings[$this->type][]=sprintf("did not check number of lines in %s (no number provided in index)",basename($file["path"]));
					}
				}
			}

			foreach($this->job["delete"] as $type => $files)
			{
				$this->_feedback(sprintf("checking lines: %s (delete file)",$type));
				$this->type = $type;

				foreach($files as $file)
				{
					$actual_lines = intval(exec("cat " . $file["tmp_path"] . " | wc -l"));

					if (isset($file["lines"]) && $file["lines"]!==false)
					{
						if ($actual_lines!==$file["lines"])
						{
							$warnings[$this->type][]=sprintf("number of lines mismatch in %s: index lists %s, file has %s",basename($file["path"]),$file["lines"],$actual_lines);
						}
					}
					else
					{
						$warnings[$this->type][]=sprintf("did not check number of lines in %s (no number provided in index)",basename($file["path"]));
					}

					$this->job["delete_files_line_count"][$this->type][]= ["file"=> basename($file["path"]), "count" => $actual_lines];
				}
			}

			$this->job["pre-validator warnings"] = $warnings;
		}

		private function _initValidator() 
		{
			$this->validator = new JsonValidator([
				'output_dir' =>	$this->outdir,
				'schema_file' => $this->cfg[$this->type]["schema_file"],
				'save_file_basename' => $this->cfg[$this->type]["save_file_basename"]
			]);

			if (isset($this->cfg[$this->type]["extra_schema"]))
			{
				$this->validator->setAdditionalJsonSchema($this->cfg[$this->type]["extra_schema"]);
			}

			# set data supplier data to overwrite corresponding values in documents (safety setting)
			if (!empty($this->cfg["supplier_codes"]["source_system_code"]) ||
				!empty($this->cfg["supplier_codes"]["source_system_name"]) ||
				!empty($this->cfg["supplier_codes"]["source_institution_id"]) ||
				!empty($this->cfg["supplier_codes"]["source_id"]))
			{
				$this->validator->setSourceSystemDefaults([
					'source_system_code'	=>	$this->cfg["supplier_codes"]["source_system_code"],
					'source_system_name'	=>	$this->cfg["supplier_codes"]["source_system_name"],
					'source_institution_id'	=>	$this->cfg["supplier_codes"]["source_institution_id"],
					'source_id'				=>	$this->cfg["supplier_codes"]["source_id"]
				]);
			}
			# or keep the originals
			else
			{
				$this->validator->setSourceSystemDefaults(false);
			}

			if (is_null($this->cfg["settings"]["read_buffer_size"]))
			{
				$read_buffer_size = self::READ_BUFFER_SIZE;
			}
			else
			{
				$read_buffer_size = (int)$this->cfg["settings"]["read_buffer_size"];
				$read_buffer_size = $read_buffer_size < 10 || $read_buffer_size > self::READ_BUFFER_SIZE ? self::READ_BUFFER_SIZE : $read_buffer_size;
			}

			$this->validator->setReadBufferSize($read_buffer_size);
			$this->validator->setLoadErrorThreshold((int)$this->cfg[$this->type]["load_error_threshold"]);
			$this->validator->setAllowDoubleIds($this->cfg[$this->type]["allow_double_ids"]);
			$this->validator->setFailOnAnyError($this->cfg[$this->type]["fail_on_any_error"]);
			$this->validator->setUseISO8601DateCheck($this->cfg["settings"]["use_ISO8601_date_check"]);
			$this->validator->setMaxOutfileLength($this->validatorMaxOutfileLength);
			$this->validator->setDocumentType($this->type);

			if (isset($this->cfg[$this->type]["export_ids"]))
			{
				$this->validator->setExportIds($this->cfg[$this->type]["export_ids"]);
			}

			if (isset($this->cfg[$this->type]["sqlite_path"]))
			{
				$this->validator->setSQLitePath($this->cfg[$this->type]["sqlite_path"]);
			}
		}

		private function _reorderFilesByIndexFile( $index_file, $files )
		{

			if(!file_exists($index_file["tmp_path"]))
			{
				$this->_feedback(sprintf("no reordening: index file %s doesn't exist",$index_file["tmp_path"]));
				return $files;
			}

			$new_order=[];

			$index_files = array_map(function($item)
				{ return str_getcsv($item, self::INDEX_FILE_FIELD_SEP); }, 
				file($index_file["tmp_path"],FILE_IGNORE_NEW_LINES|FILE_SKIP_EMPTY_LINES)
    		);

			foreach ($index_files as $index_file)
			{
				foreach ($files as $file)
				{
					if (strpos(strrev($file["path"]),strrev($index_file[0]))===0)
					{
						if ($this->ppdbWorksTopToBottom)
						{
							array_push($new_order,$file);
						}
						else
						{
							array_unshift($new_order,$file);
						}
					}
				}
			}

			if (count($files)==count($new_order))
			{
				$altered=false;
				foreach ($files as $key => $value)
				{
					if ($value["path"]!=$new_order[$key]["path"])
					{
						$altered=true;
						break;
					}
				}

				if ($altered)
				{					
					$this->_feedback("altered processing order based on index file");	
					return $new_order;
				}
			}
			else
			{
				$this->_feedback("no reordening: mismatch between index and actual files");
			}

			return $files;
		}

		private function _runValidator()
		{
			if ($this->job["use_parallel_processing"]==true)
			{
				$this->_runValidatorSerially();

 				// safety
				//$this->_runValidatorParallelly();

				/*
					TODO:
					coming in, the overall slicing status ($this->job["slicing"]["status"]) is 
					'pending'. when 'pending', _runValidatorParallelly() each time takes one
					slice, runs it through the validator, and sets the slice's status to
					'processing', then 'processed'. once all slices of a job have the status
					'processed', the overall status ($this->job["slicing"]["status"]) should
					be set to 'processed'. process_datasets.php should look at this every time
					$job_runner->run() returns, and adjust the overall job status accordingly.

					also, re-set the overall job status to pending once a slice to process has
					been selected (or none is available x2).
				*/
			}
			else
			{
				$this->_runValidatorSerially();
			}			
		}

		private function _runValidatorSerially()
		{
			$this->_feedback("using serial processing");

			$this->processed_input_files = 0;


$this->total_valid_docs = 0;
$this->total_not_valid_docs = 0;

			foreach($this->job["input"] as $type => $files)
			{
				$this->_feedback(sprintf("processing %s",$type));
				$this->type = $type;
				$this->_initValidator();

				if ($this->job["indices"][$type]!=false)
				{
					$files = $this->_reorderFilesByIndexFile($this->job["indices"][$type],$files);
				}

				foreach((array)$files as $file)
				{
					$this->validator->addFileToValidate($file["tmp_path"],$file["path"]);
				}

				$this->validator->run();

				$validator_results = $this->validator->getValidationOverview();

				foreach ((array)$validator_results["outfiles"]["valid"] as $value)
				{
					$this->job["validated_output"][] = $value;
				}

				$this->job["validator"][$this->type] = 
					[
						"settings" => $this->validator->getSettingsOverview(),
						"results" => $this->validator->getValidationOverview(),
						"error_summary" => $this->validator->getErrorSummary(),
						"double_ids" => $this->validator->getDoubleIdFilePath(),
					];

				$this->_feedback(sprintf("> %s:%s validation summary", $this->job["data_supplier"], $this->type));
				$this->_feedback(sprintf("> files; lines; errors: %s; %s; %s",
					number_format($validator_results["files_read"]),
					number_format($validator_results["lines_read"]),
					number_format($validator_results["errors"])));
				$this->_feedback( sprintf("> valid docs; invalid; broken: %s; %s; %s",
					number_format($validator_results["valid_json_docs"]),
					number_format($validator_results["invalid_json_docs"]),
					number_format($validator_results["broken_docs"])));

$this->total_valid_docs += $validator_results["valid_json_docs"];
$this->total_not_valid_docs += ($validator_results["broken_docs"] + $validator_results["invalid_json_docs"]);

$pct = ($this->total_not_valid_docs / $this->total_valid_docs) * 100;

$this->_feedback( sprintf("%s %s %s",$this->total_valid_docs,$this->total_not_valid_docs,$pct) );

				$this->processed_input_files++;
			}

			if ($this->processed_input_files==0)
			{
				$this->_feedback( "no data files were validated" );
			}
		}

		private function _runValidatorParallelly()
		{
			$this->_feedback(sprintf("using parallel processing (%s)",$this->job["slicing"]["id"]));

			if ($this->job["slicing"]["status"]!="pending")
			{
				$this->_feedback(sprintf("nothing to validate: slicing status is '%s'",$this->job["slicing"]["status"]));
				return;
			}

			foreach($this->job["slicing"]["input"] as $type => $slices)
			{
				$lowest_index=-1;
				$slice_to_process=[];

				foreach($slices as $key => $slice)
				{
					if ($slice["status"]=="pending")
					{
						foreach($slice["morsels"] as $morsel)
						{
							$morsel_index=-1;
							if ($morsel_index==-1 || $morsel["index"]<$morsel_index)
							{
								$morsel_index = $morsel["index"];
							}
						}

						if ($lowest_index==-1 || $morsel_index<$lowest_index)
						{
							$slice_to_process = $slice;
							$type_to_slice = $type;
							$slice_key = $key;
							$lowest_index = $morsel_index;
						}
					}
				}

				if ($lowest_index==-1)
				{
					$this->_feedback("nothing to validate: no slices with status 'pending'");
					return;
				}
			}

			$this->type = $type_to_slice;
			
			$this->job["slicing"]["input"][$this->type][$slice_key]["status"]="processing";
			$this->_storeUpdatedJobFile( $this->job );

			$this->_feedback(sprintf("processing %s, slice #%s",$this->type,$slice_to_process["index"]));

			$this->validator_save_file_basename = 
				$this->cfg[$this->type]["save_file_basename"] . 
				sprintf($this->job["slicing"]["file_add"],$slice_to_process["index"]);

			$this->_initValidator();

			$this->validator->setTotalDocListLength( $slice_to_process["size"] );

			foreach($slice_to_process["morsels"] as $morsel)
			{
				$file = new SplFileObject($morsel["path"]);
				if (!$file->eof())
				{
					$file->seek($morsel["start"]);
					for($i=0;$i<$morsel["size"];$i++)
					{
						$doc = $file->current();
						$this->validator->addDocToValidate( $doc, $morsel["size"]+$i, $morsel["original_path"] );
						$this->validator->runDocListValidation();
						$file->next();
					}
				}
			}

			$this->validator->runDocListValidation( true );

			$validator_results = $this->validator->getValidationOverview();

			$this->job["slicing"]["input"][$this->type][$slice_key]["validator"] = $validator_results;
			$this->job["slicing"]["input"][$this->type][$slice_key]["status"] = "processed";

			$this->_storeUpdatedJobFile( $this->job );
		}

		private function _feedback( $msg )
		{
			echo sprintf("%s\n",$msg);
		}

	}
