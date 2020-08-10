<?php

	class JobRunner
	{
		private $logClass;
		private $validatorLogClass;
		private $job;
		private $cfg;
		private $type;
		private $output_dir;
		private $validator;
		private $validatorMaxOutfileLength=500000;
		private $exportIds=false;
		private $processed_input_files=0;
		private $globalFailPercentage=-1;
		private $test_run = false;

		const READ_BUFFER_SIZE = 100000;
		const INDEX_FILE_FIELD_SEP = "\t";

		public function __construct( $job )
		{
			$this->setJob($job);
		}

		public function setLogClass($logClass)
		{
			$this->logClass = $logClass;
		}
		public function setOutputDir($output_dir)
		{
			$this->output_dir = $output_dir;
		}

		public function setValidatorMaxOutfileLength($length)
		{
			$this->validatorMaxOutfileLength = $length;
		}

		public function setExportIds($state)
		{
			$this->exportIds = $state;
		}

		public function setJob($job)
		{
			$this->job=$job;
		}

		public function getJob()
		{
			return $this->job;
		}

		public function run()
		{
			$this->_setTestRun();
			$this->_checkConfigFile();
			$this->_readConfig();
			$this->_checkNumberOfLines();
			$this->_setGlobalFailPercentage();
			$this->_setTestRunOverrides();
			$this->_runValidator();
		}

		public function storeJobFile( $job = null )
		{
			if (!is_null($job))
			{
				$this->setJob( $job );
			}

			file_put_contents($this->job["dataset_filename"], json_encode($this->job));
		}

		public function archiveValidationFiles()
		{
			if ($this->test_run)
			{
				$this->logClass->info( "test run: skipping archiving" );
				return;
			}
		
			if (!isset(["settings"]["archive_dir"]) || !file_exists($this->cfg["settings"]["archive_dir"]))
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

			$this->logClass->info(sprintf("archiving to %s",$target));

			exec("tar -zcvf " . $target . " " . $tmpdir . "/", $output);
			// print_r($output);
			foreach (array_unique($unlink_me) as $file) unlink($file);
			foreach (array_unique($rmdir_me) as $dir) rmdir($dir);

			$this->job["archived_input"]=$target;
		}

		public function moveValidatedFiles( $data_folder )
		{
			$data_folder = rtrim($data_folder,"/") . "/";

			foreach((array)$this->job["validator"] as $key=>$file)
			{
				$new_paths=[];
				
				foreach((array)$file["results"]["outfiles"]["valid"] as $file);
				{
					$new = $data_folder . basename($file);	
					if (rename($file, $new))
					{
						$new_paths[] = $new;
					}
					else
					{
						$new_paths[] = $file;
						$this->logClass->error(sprintf("couldn't move %s to %s\n",$file,$new));
					}
				}

				$this->job["validator"][$key]["results"]["outfiles"]["valid"]=$new_paths;				
			}

			foreach((array)$this->job["delete"] as $key=>$files)
			{
				$new_paths=[];

				foreach($files as $file);
				{
					$new = $data_folder . basename($file["path"]);
					if (rename($file["tmp_path"], $new))
					{
						$new_paths[] = $new;
					}
					else
					{
						$new_paths[] = $file["path"];
						$this->logClass->error(sprintf("couldn't move %s to %s\n",$file["path"],$new));
					}
				}

				$this->job["delete"][$key]=$new_paths;
			}

			$this->storeJobFile();
		}

		public function moveJobFile( $job_folder )
		{
			$job_folder = rtrim($job_folder,"/") . "/";
			$new = $job_folder . basename($this->job["dataset_filename"]);	

			if (rename($this->job["dataset_filename"], $new))
			{
				$this->job["dataset_filename"] = $new;
				$this->storeJobFile();
				$this->logClass->info(sprintf("moved job file to %s" , $this->job["dataset_filename"]));
			}
			else
			{
				$this->logClass->error(sprintf("couldn't move job file %s to %s\n",$this->job["dataset_filename"],$new));
			}
		}

		public function deleteDataFiles()
		{
			foreach (["input","delete","indices","metadata_files"] as $class)
			{
				if (!isset($this->job[$class]))
				{
					continue;
				}

				foreach($this->job[$class] as $type => $files)
				{
					if ($files===false)
					{
						continue;
					}

					foreach($files as $key => $file)
					{
						if (isset($file["tmp_path"]) && file_exists($file["tmp_path"]))
						{
							unlink($file["tmp_path"]);
							unset($this->job[$class][$type][$key]["tmp_path"]);
							$this->logClass->info(sprintf("unlinked %s",$file["tmp_path"]));							
						}
					}
				}
			}

			$this->storeJobFile();
		}


		public function moveErrorFilesToReportDir()
		{
			$result=[];

			if (!isset($this->job["validator"]))
			{
				return;
			}

			foreach($this->job["validator"] as $type=>$results)
			{
				foreach(["errors","invalid","broken"] as $state)
				{
					foreach((array)$results["results"]["outfiles"][$state] as $files)
					{
						foreach((array)$files as $file)
						{
							$in_place = false;

							foreach($this->job["report_dirs"] as $report_dir)
							{
								$sub = $report_dir . "/" . $this->job["id"];

								if (!file_exists($sub))
								{
									mkdir($sub);
								}

								$t = $sub . "/" . basename($file);
								copy($file, $t);
								$result[]=$t;
								$in_place = $in_place || $file==$t;
								$this->logClass->info(sprintf("wrote %s",$t));
							}

							if (!$in_place)
							{
								unlink($file);
							}
						}
					}
				}
			}

			$this->job["validator_client_error_files"]=$result;
			$this->storeJobFile();
		}

		public function writeClientReport()
		{
			$t=[];
			
			$t["job"] = [
				"id" => $this->job["id"],
				"job_date" => $this->job["date"],
				"data_supplier" => $this->job["data_supplier"],
				"data_supplier_export_date" => $this->job["export_date"],
				"data_supplier_notes" => $this->job["notes"],
				"validator_time_taken" => $this->job["validator_time_taken"],
				"status" => $this->job["status"],
			];

			if (isset($this->job["status_info"]))
			{
				$t["job"]["status_info"] = $this->job["status_info"];
			}

			$t["job_settings"] = [
				"tabula_rasa" => $this->job["tabula_rasa"],
				"test_run" => $this->job["test_run"],
				"is_incremental" => $this->job["is_incremental"],
			];

			if (isset($this->job["input"]))
			{
				foreach($this->job["input"] as $type=>$files)
				{
					$t["input"][$type]=[];
					foreach($files as $file)
					{
						$t["input"][$type][]=["path"=>$file["path"]];
					}		
				}
			}

			if (isset($this->job["delete"]))
			{
				foreach($this->job["delete"] as $type=>$files)
				{
					$t["delete"][$type]=[];
					foreach($files as $file)
					{
						$t["delete"][$type][]=["path"=>$file["path"]];
					}		
				}
			}

			if (isset($this->job["pre-validator warnings"]))
			{
				$t["pre-validator warnings"]=$this->job["pre-validator warnings"];
			}

			if (isset($this->job["validator"]))
			{
				foreach($this->job["validator"] as $type=>$results)
				{
					$t["validator"][$type]["settings"] = [
						"schema_file" => 
							$results["settings"]["schema_file"],
						"additional_schema_file" => 
							isset($results["settings"]["additional_schema_file"]) ? $results["settings"]["additional_schema_file"] : '-',
						"allow_double_ids" =>
							$results["settings"]["allow_double_ids"] ? 'y' : 'n',
						"use_ISO8601_date_check" =>
							$results["settings"]["use_ISO8601_date_check"] ? 'y' : 'n',
					];

					unset($results["results"]["infiles"]);
					unset($results["results"]["outfiles"]);
					unset($results["results"]["error_summary"]);

					$t["validator"][$type]["results"] = $results["results"];
				}
			}

			$files=[];

			foreach($this->job["report_dirs"] as $report_dir)
			{
				$sub = $report_dir . "/" . $this->job["id"];

				if (!file_exists($sub))
				{
					mkdir($sub);
				}
				
				$f = $sub . "/" . $this->job["id"] . "--report.json";

				file_put_contents($f, json_encode($t));

				$files[]=$f;
			}

			$this->job["validator_client_reports"]=$files;
			$this->storeJobFile();
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
				$this->logClass->info(sprintf("checking lines: %s",$type));
				$this->type = $type;

				foreach($files as $file)
				{
					if (isset($file["lines"]) && $file["lines"]!==false)
					{
						$actual_lines = intval(exec("cat " . $file["tmp_path"] . " | wc -l"));
						if ($actual_lines!==$file["lines"])
						{
							$line =
								sprintf("number of lines mismatch in %s: index lists %s, file has %s",
									basename($file["path"]),
									$file["lines"],
									$actual_lines
								);
							$warnings[$this->type][] = $line;
							$this->logClass->warning($line);
						}
					}
					else
					{
						$line = 
							sprintf("did not check number of lines in %s (no number provided in index)",basename($file["path"]));
						$warnings[$this->type][] = $line;
						$this->logClass->warning($line);
					}
				}
			}

			foreach($this->job["delete"] as $type => $files)
			{
				$this->logClass->info(sprintf("checking lines: %s (delete file)",$type));
				$this->type = $type;

				foreach($files as $file)
				{
					$actual_lines = intval(exec("cat " . $file["tmp_path"] . " | wc -l"));

					if (isset($file["lines"]) && $file["lines"]!==false)
					{
						if ($actual_lines!==$file["lines"])
						{
							$line = 
								sprintf("number of lines mismatch in %s: index lists %s, file has %s",
									basename($file["path"]),
									$file["lines"],
									$actual_lines
								);
							$warnings[$this->type][] = $line;
							$this->logClass->warning($line);
						}
					}
					else
					{
						$line =
							sprintf("did not check number of lines in %s (no number provided in index)",basename($file["path"]));
						$warnings[$this->type][] = $line;
						$this->logClass->warning($line);
					}

					$this->job["delete_files_line_count"][$this->type][]= ["file"=> basename($file["path"]), "count" => $actual_lines];
				}
			}

			$this->job["pre-validator warnings"] = $warnings;
		}

		private function _initValidator() 
		{
			$this->validator = new JsonValidator([
				'output_dir' =>	$this->output_dir,
				'schema_file' => $this->cfg[$this->type]["schema_file"],
				'data_type' => $this->type,
				'data_supplier' => $this->job["data_supplier"],
				'job_id' => $this->job["id"],
				'test_run' => $this->test_run
			]);

			$this->validatorLogClass = new LogClass("log/validator.log","validator");
			$this->validator->setLogClass($this->validatorLogClass);

			if (isset($this->cfg[$this->type]["extra_schema"]))
			{
				$this->validator->setAdditionalJsonSchema($this->cfg[$this->type]["extra_schema"]);
			}


			$this->validator->setSourceSystemDefaults(false);

			foreach( [ "source_system_code", "source_system_name", "source_institution_id", "source_id" ] as $key)
			{
				if (!empty($this->cfg["supplier_codes"][$key]))
				{
					$this->validator->setSourceSystemDefaults([
						$key =>	$this->cfg["supplier_codes"][$key],
					]);
				}
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
				$this->logClass->info(sprintf("no reordening: index file %s doesn't exist",$index_file["tmp_path"]));
				return $files;
			}

			$new_order=[];

			$indexed_files = array_map(function($item)
				{ return str_getcsv($item, self::INDEX_FILE_FIELD_SEP); }, 
				file($index_file["tmp_path"],FILE_IGNORE_NEW_LINES|FILE_SKIP_EMPTY_LINES)
    		);

			foreach ($indexed_files as $indexed_file)
			{
				foreach ($files as $file)
				{
					if (strpos(strrev($file["path"]),strrev($indexed_file[0]))===0)
					{
						array_push($new_order,$file);
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
					$this->logClass->info("altered processing order based on index file");	
					return $new_order;
				}
			}
			else
			{
				$this->logClass->info("no reordening: mismatch between index and actual files");
			}

			return $files;
		}

		private function _setGlobalFailPercentage()
		{
			$this->globalFailPercentage = $this->cfg["settings"]["global_fail_percentage"] ?? -1;
			$this->logClass->info( $this->globalFailPercentage==-1 ? "no global fail percentage set" : sprintf("global fail percentage set at %s%%",$this->globalFailPercentage) );
		}

		private function _getGlobalFailedPercentage()
		{
			if ($this->total_valid_docs==0)
			{
				return 0;
			}
			else
			{
				return round(($this->total_not_valid_docs / $this->total_valid_docs) * 100,2);	
			}
		}

		private function _setTestRun()
		{
			$this->test_run = isset($this->job["test_run"]) && is_bool($this->job["test_run"]) ? $this->job["test_run"] : false;
			$this->logClass->info( $this->test_run  ? "this is a test run" : "this is not a test run" );
		}

		private function _runValidator()
		{
			$this->processed_input_files = 0;
			$this->total_valid_docs = 0;
			$this->total_not_valid_docs = 0;

			foreach($this->job["input"] as $type => $files)
			{
				$this->logClass->info(sprintf("processing %s",$type));
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

				$this->logClass->info(sprintf("%s/%s validation: %s files, %s lines",
					$this->job["data_supplier"], 
					$this->type,
					number_format($validator_results["files_read"]),
					number_format($validator_results["lines_read"])
				));

				$this->logClass->info(sprintf("%s/%s: %s valid docs, %s invalid, %s broken",
					$this->job["data_supplier"], 
					$this->type,
					number_format($validator_results["valid_json_docs"]),
					number_format($validator_results["invalid_json_docs"]),
					number_format($validator_results["broken_docs"])
				));

				$this->total_valid_docs += $validator_results["valid_json_docs"];
				$this->total_not_valid_docs += ($validator_results["broken_docs"] + $validator_results["invalid_json_docs"]);
				$this->processed_input_files += count($files);

				$this->logClass->info(sprintf("%s/%s: processed %s files (%s total); failed %s%%",
					$this->job["data_supplier"], 
					$this->type,
					count($this->job["input"][$this->type]),
					$this->processed_input_files,
					$this->_getGlobalFailedPercentage())
				);
			}

			if ($this->processed_input_files==0)
			{
				$this->logClass->info("no data files were validated");
			}
			else
			{
				$this->_checkGlobalFailureConditions();
			}
		}

		private function _checkGlobalFailureConditions()
		{
			if ($this->globalFailPercentage>=0 && $this->_getGlobalFailedPercentage() >= $this->globalFailPercentage)
			{
				throw new Exception(sprintf("validation of %s%% of documents in job failed (threshold: %s%%)",$this->_getGlobalFailedPercentage(),$this->globalFailPercentage));
			}
		}

		private function _setTestRunOverrides()
		{
			if (!$this->test_run)
			{
				return;
			}

			$this->globalFailPercentage = 101;
			$this->logClass->info(sprintf("test run override: global fail percentage  => %s%%",$this->globalFailPercentage));
		}

	}
