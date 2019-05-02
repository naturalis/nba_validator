<?php

	function getConfigFile()
	{
		$cfg = getopt("",["config:"]);

		if (empty($cfg) || empty($cfg["config"]))
		{
			print "need a config file\n";
			exit(0);
		}

		return realpath($cfg["config"]);
	}

	function readConfig( $file=false )
	{
		if (!$file)
		{
			$cfg = getConfigFile();
		}
		else 
		{
			$cfg = $file;
		}

		if (!file_exists($cfg))
		{
			print sprintf("config file %s doesn't exist\n",$cfg);
			exit(0);
		}
		else
		{
			$cfg = parse_ini_file($cfg,true,INI_SCANNER_TYPED);

			if ($cfg==false)
			{
				print sprintf("can't read config file %s\n",$cfg);
				exit(0);				
			}
		}

		return $cfg;
	}

	function secondsToTime( $seconds )
	{
	  $hours = 0;
	  $milliseconds = str_replace( "0.", '', $seconds - floor( $seconds ) );
	  $milliseconds = substr($milliseconds,0,5);

	  if ( $seconds > 3600 )
	  {
	    $hours = floor( $seconds / 3600 );
	  }
	  $seconds = $seconds % 3600;

	  return str_pad( $hours, 2, '0', STR_PAD_LEFT )
	       . gmdate( ':i:s', $seconds )
	       . ($milliseconds ? ".".$milliseconds : '')
	  ;
	}

	function readRepository( $repopath )
	{
		$repository = glob(rtrim($repopath,"/") . "/*.json");
		usort($repository, create_function('$a,$b', 'return filemtime($a) - filemtime($b);'));
		return $repository;
	}

	function getOutDir()
	{
		$cfg = getopt("",["outdir:"]);

		if (empty($cfg) || empty($cfg["outdir"]) || !file_exists($cfg["outdir"]))
		{
			print "need an outdir\n";
			exit(0);
		}

		return realpath($cfg["outdir"]);
	}

	function writeClientReport( $job )
	{
		$t=[];
		
		$t["job"] = [
			"id" => $job["id"],
			"date" => $job["date"],
			"status" => $job["status"]
		];

		if (isset($job["status_info"]))
		{
			$t["job"]["status_info"] = $job["status_info"];
		}

		$t["settings"] =
			[ "is_incremental" => 
				[ "specimen" => $job["is_incremental"]["specimen"],
				  "multimedia" => $job["is_incremental"]["multimedia"] ] ];

		if (isset($job["input"]))
		{
			foreach($job["input"] as $type=>$files)
			{
				$t["input"][$type]=[];
				foreach($files as $file)
				{
					$t["input"][$type][]=["path"=>$file["path"]];
				}		
			}
		}

		if (isset($job["pre-validator warnings"]))
		{
			$t["pre-validator warnings"]=$job["pre-validator warnings"];
		}

		if (isset($job["validator"]))
		{
			foreach($job["validator"] as $type=>$results)
			{
				unset($results["results"]["infiles"]);
				unset($results["results"]["outfiles"]);
			
				$t["validator"][$type]=$results["results"];
			}
		}

		$t["error_files"] = $job["validator_client_error_files"];

		$files=[];

		foreach($job["report_dirs"] as $report_dir)
		{
			$f = $report_dir . "/" . $job["id"] . "--report.json";
			file_put_contents($f, json_encode($t));
			$files[]=$f;
		}

		return $files;
	}

	function moveErrorFilesToReportDir( $job )
	{
		$result=[];

		if (!isset($job["validator"]))
		{
			return $result;
		}

		foreach($job["validator"] as $type=>$results)
		{
			foreach(["errors","invalid","broken"] as $state)
			{
				foreach((array)$results["results"]["outfiles"][$state] as $files)
				{
					foreach((array)$files as $file)
					{
						foreach($job["report_dirs"] as $report_dir)
						{
							$t = $report_dir . "/" . basename($file);
							copy($file, $t);
							$result[]=$t;
						}

						unlink($file);
					}
				}
			}
		}

		return $result;
	}

	function putValidationLog( $job, $elastic_log_server )
	{
		if (is_null($elastic_log_server))
		{
			print "elastic_log_server is empty\n";
		}

		$tpl = [
			"@timestamp" => $job["date"],
			"job_id" => $job["id"],
			"data_supplier" => $job["data_supplier"],
			"index_type" => "",
			"input_files" => [],
			"files_read" => 0,
			"lines_read" => 0,
			"valid_json_docs" => 0,
			"broken_docs" => 0,
			"invalid_json_docs" => 0,
			"ids_total" => 0,
			"ids_unique" => 0,
			"errors" => 0,
			"error_summary" => [],			
		];

		foreach(["specimen","multimedia","taxon","geo"] as $index_type)
		{
			$doc = $tpl;
			$doc["index_type"] = $index_type;

			if (isset($job["validator"]) && isset($job["validator"][$index_type]["results"]))
			{
				$t = $job["validator"][$index_type]["results"];
				$doc["files_read"] = $t["files_read"];
				$doc["lines_read"] = $t["lines_read"];
				$doc["valid_json_docs"] = $t["valid_json_docs"];
				$doc["invalid_json_docs"] = $t["invalid_json_docs"];
				$doc["broken_docs"] = $t["broken_docs"];
				$doc["total_errors"] = $t["errors"];
				$doc["ids_total"] = $t["ids_total"]=='n/a' ? -1 : $t["ids_total"];
				$doc["ids_unique"] = $t["ids_unique"]=='n/a' ? -1 : $t["ids_unique"];

				if (isset($job["validator"][$index_type]["input_files"]))
				{
					foreach ($job["validator"][$index_type]["input_files"] as $file)
					{
						$doc["input_files"][] = basename($file["original_name"]);
					}
				}
			}

			$doc = json_encode($doc);
			$url = rtrim($elastic_log_server,"/") . "/" . $job["id"] . "/logging/validation-" . $index_type;

			$response = postHttpDocument( $doc, $url, "PUT" );

			if ($response!==true)
			{
				print sprintf("logger error: %s (%s)\n" , ($response ? $response : '(no response; server unreachable?)'),$url);
			}
		}
	}

	function storeUpdatedJobFile( $job )
	{
		file_put_contents($job["dataset_filename"], json_encode($job));
	}

	function moveJobFilesToPercolator( $job, $preprocessor_job_folder, $preprocessor_file_folder )
	{
		if (!file_exists($preprocessor_job_folder))
		{
			echo sprintf("folder doesn't exist: %s\n",$preprocessor_job_folder);
			return;
		}
		if (!file_exists($preprocessor_file_folder))
		{
			echo sprintf("folder doesn't exist: %s\n",$preprocessor_file_folder);
			return;
		}

		$preprocessor_job_folder = rtrim($preprocessor_job_folder,"/") . "/";
		$preprocessor_file_folder = rtrim($preprocessor_file_folder,"/") . "/";

		foreach((array)$job["validator"] as $key=>$file)
		{
			$new_paths=[];
			
			foreach($file["results"]["outfiles"]["valid"] as $file);
			{
				$new = $preprocessor_file_folder . basename($file);	
				if (rename($file, $new))
				{
					$new_paths[] = $new;
				}
				else
				{
					$new_paths[] = $file;
					echo sprintf("couldn't move %s to %s\n",$file,$new);
				}
			}

			$job["validator"][$key]["results"]["outfiles"]["valid"]=$new_paths;
			
		}

		foreach((array)$job["delete"] as $key=>$files)
		{
			$new_paths=[];
			foreach($files as $file);
			{
				$new = $preprocessor_file_folder . basename($file["path"]);
				if (rename($file["tmp_path"], $new))
				{
					$new_paths[] = $new;
				}
				else
				{
					$new_paths[] = $file["path"];
					echo sprintf("couldn't move %s to %s\n",$file["path"],$new);
				}
			}

			$job["delete"][$key]=$new_paths;
		}

		storeUpdatedJobFile( $job );

		$new = $preprocessor_job_folder . basename($job["dataset_filename"]);	

		if (copy($job["dataset_filename"], $new))
		{
			$job["dataset_filename"] = $new;
			storeUpdatedJobFile( $job );
		}
		else
		{
			echo sprintf("couldn't copy job file %s to %s\n",$job["dataset_filename"],$new);			
		}

		return $job;
	}

	function postSlackJobResults( $slack_hook, $job, $include_error_summary = false )
	{
		$d=[];

		$d[]=  sprintf("*validator* completed job *`%s`* for *%s* with status *%s* (took %s):",
			$job["id"], $job["data_supplier"], $job["status"], $job["validator_time_taken"]);

		if (!is_null($job["status_info"]))
		{
			$d[] = sprintf("_status info:_ %s",$job["status_info"]);
		}

		if (isset($job["input"]))
		{
			foreach ($job["input"] as $type => $files)
			{
				$d[] = sprintf("_%s_ data files:",$type);

				foreach ($files as $file)
				{
					$d[] =sprintf("> %s",basename($file["path"]));
				}
			}
		}

		if (isset($job["delete"]))
		{
			foreach ($job["delete"] as $type => $files)
			{
				$d[] = sprintf("_%s_ delete files:",$type);

				foreach ($files as $file)
				{
					$d[] =sprintf("> %s",basename($file["path"]));
				}
			}
		}

		$error_summary=[];

		if (isset($job["validator"]))
		{
			$d[] = sprintf("_validation overview_:");

			foreach ($job["validator"] as $type => $val)
			{
				$d[] = sprintf("> %s: %s valid docs, %s invalid, %s broken",
					$type,
					number_format($val["results"]["valid_json_docs"]),
					number_format($val["results"]["invalid_json_docs"]),
					number_format($val["results"]["broken_docs"])
				);

				if ($val["results"]["invalid_json_docs"]>0)
				{
					$error_summary[$type][]=$val["error_summary"];
				}
			}
		}

		if (isset($job["delete_files_line_count"]))
		{
			$d[] = sprintf("_delete file overview_:");

			foreach ($job["delete_files_line_count"] as $type => $files)
			{
				foreach ($files as $val)
				{
					$d[] = sprintf("> %s: found delete file `%s` with %s lines", $type, $val["file"], number_format($val["count"]));
				}
			}
		}

		$doc = implode("\n", $d);

		if ($include_error_summary && !empty($error_summary))
		{
			$doc = $doc . "\n\n" . sprintf("error summary:\n```%s```",print_r($error_summary,true)). "\n";
		}

		$doc = $doc . "\n" . sprintf("_---validator job %s report end_",$job["id"]) ;

		return postHttpDocument( json_encode([ "text" => $doc]), $slack_hook );
	}

	function postHttpDocument( $doc, $url, $method = "POST" )
	{
		$ch = curl_init($url);

		curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
		curl_setopt($ch, CURLOPT_POSTFIELDS,$doc);

		$response = curl_exec($ch);

		if ($response) 
		{
			try {
		    	$r=json_decode($response);
		    	if (isset($r->error) )
		    	{
		    		$r = $r->error;
		    	}
		    	else
		    	{
		    		$r = true;
		    	}
		    } catch (Exception $e)
		    {
		    	$r = is_object($response) ? json_encode($response) : $response;
		    }
		}

		curl_close($ch);
		return $r;
	}
