<?php

	class dataSet
	{
		private $logClass;
		private $dirs=[];
		private $files_are_present=false;
		private $hash_val="";
		private $data_supplier_code="";
		private $output_dir;
		private $report_dirs=[];
		private $set=[];
		private $dataset_filename="";
		private $tmp_dir = "/tmp/validator/";
		private $supplier_config_file="";
		private $is_incremental;
		private $present_datatypes=[];

		private $always_process=false;
		private $force_data_replace=false;
		private $is_test_run=false;

		private $job_id_override;
		private $job_date_override;
		private $inherited_metadata=[];

		const JSON_EXTENSIONS = ["json","JSON","jsonl","JSONL","ndjson","NDJSON"];
		const FILE_UPLOAD_READY = "upload_ready";
		const FILE_PROCESSING = "processing";
		const INDEX_FILE_MASK = "index-*.txt";
		const DELETE_FILE_MASK = "delete*-*.txt";
		const METADATA_FILE_MASK = "metadata-%s.json";
		const DATA_TYPES = ["specimen","multimedia","taxon","geo"];
		const INDEX_FILE_FIELD_SEP = "\t";
		

		public function makeDataset()
		{
			$this->_getTimeStamp();
			$this->_preliminaries();
			$this->_createTmpPath();
			$this->_readFlags();
			$this->_setProcessStatus();
			$this->_getIndexFile();
			$this->_getMetaDataFile();
			$this->_getJsonFiles();
			$this->_getDeleteFiles();
			$this->_checkIfFilesArePresent();
			// $this->_getFileCreationTimes();
			$this->_readIndexFile();
			$this->_setImportDateAndNotes();
			$this->_generateHash();
			$this->_makeSet();
			$this->_writeSet();
		}

		public function setLogClass($logClass)
		{
			$this->logClass = $logClass;
		}

		public function addInputDirectory( $dir, $dataType )
		{
			if (!in_array($dataType,self::DATA_TYPES))
			{
				throw new Exception(sprintf("unknown datatype %s",$dataType));
			}

			if (file_exists($dir))
			{
				array_push($this->dirs,["path"=>realpath($dir),"do_process"=>false,"json"=>[],"type"=>$dataType]);
				$this->present_datatypes[]=$dataType;
			}
			else
			{
				throw new Exception(sprintf("directory %s doesn't exist",$dir));
			}		
		}

		public function setDataSupplierCode( $code )
		{
			$code = strval($code);
			if (strlen($code)<2 || strlen($code)>6)
			{
				throw new Exception("data supplier code should be 2..6 characters");
			}
			$this->data_supplier_code = $code;
		}

		public function setOutputDirectory( $dir )
		{
			if (!file_exists($dir))
			{
				throw new Exception(sprintf("directory %s doesn't exist",$dir));
			}
			$this->output_dir = realpath($dir);
		}

		public function setReportDirectory( $dir )
		{
			if (is_array($dir))
			{
				foreach ($dir as $val)
				{
					$this->_doSetReportDirectory( $val );
				}
			}
			else
			{
				$this->_doSetReportDirectory( $dir );
			}
		}

		public function setTmpDirectory( $dir )
		{
			if (!file_exists($dir))
			{
				throw new Exception(sprintf("directory %s doesn't exist",$dir));
			}
			$this->tmp_dir = realpath($dir);
		}

		public function setIsIncremental( $is_incremental, $dataType )
		{
			if (!in_array($dataType,self::DATA_TYPES))
			{
				throw new Exception(sprintf("unknown datatype %s (setIsIncremental)",$dataType));
			}

			if (is_bool($is_incremental))
			{
				$this->is_incremental[$dataType] = $is_incremental;
			}
		}

		public function setSupplierConfigFile( $file )
		{
			$this->supplier_config_file = $file;
		}

		public function setForceDataReplace( $state )
		{
			if (is_bool($state))
			{
				$this->force_data_replace = $state;	
			}
		}

		public function setIsTestRun($is_test_run)
		{
			if (is_bool($is_test_run))
			{
				$this->is_test_run = $is_test_run;
			}
		}

		public function addInheritedMetadata($source,$name_value)
		{
			$this->inherited_metadata[$source][] = $name_value;
		}

		public function getDatasetFilename()
		{
			return $this->dataset_filename;
		}

		public function prepareSetForProcessing( $copyNotMove = false )
		{
			foreach($this->dirs as $dir)
			{
				if ($dir["do_process"])
				{
					unlink($dir["path"] . "/" . self::FILE_UPLOAD_READY);
					touch($dir["path"] . "/" . self::FILE_PROCESSING);
				}
			}

			foreach(self::DATA_TYPES as $dataType)
			{
				$this->_moveFiles($dataType, $copyNotMove);
			}

			$this->_writeSet();
		}

		public function removeProcessingFlags()
		{
			foreach($this->dirs as $dir)
			{
				if ($dir["do_process"])
				{
					unlink($dir["path"] . "/" . self::FILE_PROCESSING);
				}
			}
		}

		public function setChangedNames( $index )
		{
			$this->changed_file_names=$index;
		}

		public function setJobIdOverride($job_id_override)
		{
			$this->job_id_override = $job_id_override;
		}

		public function setJobDateOverride($job_date_override)
		{
			$this->job_date_override = $job_date_override;
		}

		private function _preliminaries()
		{
			if (count($this->dirs)==0)
			{
				throw new Exception("no directories provided");
			}
			if (strlen($this->data_supplier_code)==0)
			{
				throw new Exception("no data supplier code provided");
			}
			if (strlen($this->output_dir)==0)
			{
				throw new Exception("no output directory provided");
			}
			if (count($this->report_dirs)==0)
			{
				throw new Exception("no report directory provided");
			}

			foreach($this->present_datatypes as $type)
			{
				if (!isset($this->is_incremental[$type]))
				{
					throw new Exception(sprintf("no value set for incremental/full for %s",$type));
				}
			}
		}

		private function _readFlags()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir)
			{
				$this->dirs[$key]["upload_ready"]=file_exists($dir["path"] . "/" . self::FILE_UPLOAD_READY);
				$this->dirs[$key]["processing"]=file_exists($dir["path"] . "/" . self::FILE_PROCESSING);
				$this->logClass->info(
					sprintf("%s, %s: %s", $dir["path"], self::FILE_UPLOAD_READY,$this->dirs[$key]["upload_ready"] ? 'y' : 'n'));
				$this->logClass->info(
					sprintf("%s, %s: %s", $dir["path"], self::FILE_PROCESSING,$this->dirs[$key]["upload_ready"] ? 'y' : 'n'));
			}
		}

		private function _setProcessStatus()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir)
			{
				$this->dirs[$key]["do_process"]=($dir["upload_ready"] && !$dir["processing"]) || $this->always_process;
				$this->logClass->info(sprintf("do process %s? %s", $dir["path"], $this->dirs[$key]["do_process"] ? 'y' : 'n'));
			}
		}

		private function _getIndexFile()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir) 
			{
				if ($dir["do_process"])
				{
					$f=glob($dir["path"] . "/" . self::INDEX_FILE_MASK);

					if (count($f)>1)
					{
						throw new Exception(sprintf("multiple index files found in %s",$dir["path"]));
					}
					else
					{
						$this->dirs[$key]["index_file"] = ( count($f)==1 ? $f[0] : false );
					}
				}
			}
		}

		private function _getMetaDataFile()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir) 
			{
				if ($dir["do_process"])
				{
					$f=glob($dir["path"] . "/" . sprintf(self::METADATA_FILE_MASK,$dir["type"]));
					$this->dirs[$key]["metadata_file"] = ( count($f)==1 ? $f[0] : false );
				}
			}
		}

		private function _getJsonFiles()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir) 
			{
				if ($dir["do_process"])
				{
					$files = $this->getFileList($dir["path"],self::JSON_EXTENSIONS);
					$files = array_diff($files, $dir["metadata_file"] ? [ $dir["metadata_file"] ] : []);

					// $this->dirs[$key]["json"]=glob($dir["path"]."/*.{".implode(",",self::JSON_EXTENSIONS)."}", GLOB_BRACE);
					$this->dirs[$key]["json"]=$files;
					$this->files_are_present = $this->files_are_present ? true : count($this->dirs[$key]["json"])>0;

					$this->logClass->info(sprintf("%s, %s json file(s)", $dir["path"], count($files)));
				}
			}
		}

		private function _getDeleteFiles()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir) 
			{
				$files=glob($dir["path"] . "/" . self::DELETE_FILE_MASK);

				if ($dir["do_process"])
				{
					$this->dirs[$key]["delete"]=$files;
					$this->files_are_present = $this->files_are_present ? true : count($this->dirs[$key]["delete"])>0;
				}

				$this->logClass->info(sprintf("%s, %s delete file(s)", $dir["path"], count($files)));
			}
		}

		private function _getFileCreationTimes()
		{
			$t=&$this->dirs;
			foreach ($t as $key => $dir)
			{
				foreach ($dir["json"] as $key2 => $file)
				{
					$this->dirs[$key]["stats"][$key2] = date("Y-m-d\TH:i:s\Z", stat($file)["mtime"]);
				}
			}
		}

		private function _checkIfFilesArePresent()
		{
			if (!$this->files_are_present)
			{
				throw new Exception(
					sprintf("cannot make dataset: no files found, `%s` not present or `%s` present",self::FILE_UPLOAD_READY,self::FILE_PROCESSING)
				);
			}
		}

		private function _readIndexFile()
		{
			foreach($this->dirs as $key => $dir)
			{
				if(isset($dir["index_file"]) && $dir["index_file"]!=false)
				{
					$h = @fopen($dir["index_file"], "r");
					if ($h)
					{
					    while (($buffer=fgets($h,4096)) !== false)
					    {
					    	$b=explode(self::INDEX_FILE_FIELD_SEP,trim($buffer));
					    	if (count($b)==2)
					    	{
					    		$this->dirs[$key]["index"][]=["file"=>trim($b[0]),"lines"=>intval($b[1])];
					    	}
					    }
					    fclose($h);
					}
				}
			}
		}

		private function _setImportDateAndNotes()
		{
			foreach($this->dirs as $key => $dir)
			{
				$export_date = null;
				$source = null;
				$notes = null;

				if(isset($dir["metadata_file"]) && $dir["metadata_file"]!=false)
				{
					$data = json_decode(file_get_contents($dir["metadata_file"]),true);
					$export_date = $data["export_date"] ?? null;
					$notes = $data["notes"] ?? null;
					$source = "metadata";						
				}

				if (is_null($export_date) && isset($dir["json"][0]))
				{
					$basename = basename($dir["json"][0]);

					foreach (
						[
                            '/2[0-9]{3}\-[0-9]{2}\-[0-9]{2}/', // a cheery wave to 22nd century coders!
                            '/2[0-9]{7}/',
                            '/[0-9]{2}\-[0-9]{2}\-2[0-9]{3}/',
						] as $reg)
					{
						preg_match($reg,$basename,$matches);

						if (count($matches)>0)
						{
							$export_date = $matches[0];
							$source = sprintf("filename (first file: %s)",$basename);
							break;
						}
					}
				}

				if (isset($export_date) && strtotime($export_date)!==false)
				{
					$export_date = date("Y-m-d",strtotime($export_date));
				}
				else
				{
					$export_date = date("Y-m-d",$this->timestamp);
					$source = "none (jobfile date)";
				}

				$this->dirs[$key]["export_date"] = [ "date" => $export_date, "source" => $source ];
				$this->dirs[$key]["notes"] = $notes;
			}
		}

		private function _getPreRenameFilename( $current_file_name )
		{
			foreach ((array)$this->changed_file_names as $val)
			{
				if ($val["new"]==$current_file_name)
				{
					return $val["old"];
				}
			}
			return $current_file_name;
		}

		private function _getFileLines( $file, $index )
		{
			foreach ((array)$index as $val)
			{
				if($val["file"]==pathinfo($file,PATHINFO_BASENAME))
				{
					return $val["lines"];
				}
			}
			return false;
		}

		private function _getTimeStamp()
		{
			$this->timestamp = microtime(true);
		}

		private function _generateHash()
		{
			$this->hash_val = hash("sha256", $this->data_supplier_code . strval($this->timestamp));
			$this->hash_val = substr($this->hash_val,0,8);
		}

		private function _makeSet()
		{
			$t = DateTime::createFromFormat('U.u', $this->timestamp);
			
			if (isset($this->job_id_override))
			{
				$this->set["id"] = $this->job_id_override;
			}
			else
			{
				$this->set["id"] = implode("-",
					[strtolower($this->data_supplier_code),$t->format("Y-m-d-Hisv"),$this->hash_val]);
			}

			if (isset($this->job_date_override))
			{
				$this->set["date"] = $this->job_date_override;
			}
			else
			{
				$this->set["date"] = $t->format("c");
			}

			$this->set["data_supplier"] = $this->data_supplier_code;
			$this->set["status"] = "pending";
			$this->set["supplier_config_file"] = $this->supplier_config_file;
			$this->set["tabula_rasa"] = $this->force_data_replace;
			$this->set["test_run"] = $this->is_test_run;

			foreach($this->present_datatypes as $type)
			{
				$this->set["is_incremental"][$type] = $this->is_incremental[$type];
			}

			$this->set["report_dirs"] = $this->report_dirs;
			$this->set["changed_file_names"] = $this->changed_file_names;

			$files = [];
			$deletes = [];
			$indices = [];
			$metadata_files = [];

			foreach($this->present_datatypes as $type)
			{
				$indices[$type] = false;
			}

			foreach($this->dirs as $key => $dir)
			{
				if (isset($dir["json"]))
				{
					foreach((array)$dir["json"] as $key2 => $file)
					{
						$t = ["path" => $file,"path_hash" => md5($file)];

						if (isset($dir["index"]))
						{
							$t["lines"] = $this->_getFileLines($this->_getPreRenameFilename($file),$dir["index"]);
						}

						$files[$dir["type"]][] = $t;
					}
				}

				if (isset($dir["delete"]))
				{
					foreach((array)$dir["delete"] as $key2 => $file)
					{
						$t = ["path" => $file,"path_hash" => md5($file)];

						if (isset($dir["index"]))
						{
							$t["lines"] = $this->_getFileLines($this->_getPreRenameFilename($file),$dir["index"]);
						}

						$deletes[$dir["type"]][] = $t;
					}
				}

				$indices[$dir["type"]]=$dir["index_file"];
				$metadata_files[$dir["type"]]=$dir["metadata_file"];

				$this->set["export_date"][$dir["type"]] = $dir["export_date"];
				$this->set["notes"][$dir["type"]] = $dir["notes"];
			}
			
			$this->set["input"] = $files;
			$this->set["delete"] = $deletes;
			$this->set["indices"] = $indices;
			$this->set["metadata_files"] = $metadata_files;
			$this->set["inherited_metadata"] = $this->inherited_metadata;
		}

		private function _writeSet()
		{
			$this->dataset_filename = $this->output_dir . "/" . $this->set["id"] . ".json";
			$this->set["dataset_filename"] = $this->dataset_filename;
			if (file_put_contents($this->dataset_filename, json_encode($this->set))===false)
			{
				throw new Exception(sprintf("error writing %s",$this->dataset_filename));
			}
			else
			{
				// echo sprintf("wrote %s\n",$this->dataset_filename);
			}
		}

		private function _createTmpPath()
		{
			if (!file_exists($this->tmp_dir))
			{
				mkdir($this->tmp_dir, 0777, true);
			}
		}

		private function _abbreviateDataType( $dataType )
		{
			switch(strtolower($dataType))
			{
				case "specimen":
					return "sp";
					break;
				case "multimedia":
					return "mm";
					break;
				case "taxon":
					return "tx";
					break;
				default:
					return $dataType;
			}
		}

		private function _abbreviateDocType( $docType )
		{
			switch(strtolower($docType))
			{
				case "input":
					return "data";
					break;
				case "delete":
					return "del";
					break;
				default:
					return $docType;
			}
		}

		private function _makeTmpFilename( $setId, $dataType, $docType, $pathHash )
		{

			return sprintf("%s/%s--%s.%s.%s",realpath($this->tmp_dir),$setId,$this->_abbreviateDataType($dataType),$this->_abbreviateDocType($docType),$pathHash);
		}

		private function _moveFiles( $dataType, $copyNotMove = false )
		{
			$t=&$this->set;

			foreach(["input","delete","indices","metadata_files"] as $docType)
			{
				if (isset($t[$docType][$dataType]) )
				{
					if (is_array($t[$docType][$dataType]))
					{
						foreach ($t[$docType][$dataType] as $key => $val)
						{
							$tmp=$this->_makeTmpFilename($this->set["id"], $dataType, $docType, $val['path_hash']);

							if ($copyNotMove ? copy($val['path'], $tmp) : rename($val['path'], $tmp))
							{
								$this->set[$docType][$dataType][$key]["tmp_path"] = $tmp;
							}
							else
							{
								$this->set[$docType][$dataType][$key]["tmp_path"] = false;
							}
						}
					}
					else
					{
						$f=$t[$docType][$dataType];
						if ($f==false) continue;

						$tmp=$this->_makeTmpFilename($this->set["id"], $dataType, $docType, md5($f));

						if ($copyNotMove ? copy($f, $tmp) : rename($f, $tmp))
						{
							$this->set[$docType][$dataType]=[ "path" => $f, "tmp_path" => $tmp];
						}
						else
						{
							$this->set[$docType][$dataType]=[ "path" => $f, "tmp_path" => false];
						}						
					}
				}
			}
		}

		private function getFileList( $file_path, $extensions )
		{
			$files=[];
		
			if ($handle = opendir($file_path))
			{
				while (false !== ($entry = readdir($handle)))
				{
                    if (preg_match('/\.(' . implode('|',$extensions) .')$/', $entry))
					{
						$files[] = rtrim($file_path, "/") . "/" . $entry;
					}
				}
				closedir($handle);
			}

			return $files;
		}

		private function _doSetReportDirectory( $dir )
		{
			if (!file_exists($dir))
			{
				throw new Exception(sprintf("report directory %s doesn't exist",$dir));
			}
			$this->report_dirs[] = realpath($dir);								
		}

	}
