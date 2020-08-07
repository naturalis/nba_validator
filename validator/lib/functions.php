<?php

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

	function rmDirRecursive($dir)
	{
	    $it = new RecursiveDirectoryIterator($dir, RecursiveDirectoryIterator::SKIP_DOTS);
	    $files = new RecursiveIteratorIterator($it,
	                 RecursiveIteratorIterator::CHILD_FIRST);
	    foreach($files as $file)
	    {
	        if ($file->isDir())
	        {
	            rmdir($file->getRealPath());
	        } 
	        else
	        {
	            unlink($file->getRealPath());
	        }
	    }
	    rmdir($dir);
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

	function postSlackJobResults( $slack_hook, $job )
	{
		$d=[];

		$d[]=  sprintf("*validator* completed job *`%s`* for *%s* with status *%s* (took %s):",
			$job["id"], $job["data_supplier"], $job["status"], $job["validator_time_taken"]);

		if (!is_null($job["status_info"]))
		{
			$d[] = sprintf("_status info:_ %s",$job["status_info"]);
		}

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
		$doc = $doc . "\n" . sprintf("_---validator report end %s_",$job["id"]) ;

		return postHttpDocument( json_encode([ "text" => $doc]), $slack_hook );
	}


