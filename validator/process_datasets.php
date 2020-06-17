<?php


/*
		dry run


		reead archiving from jobfile

		// set no archiving as switch if dry run
// set no archiving at all (def false, make switcable)




	*/



	require __DIR__ . '/vendor/autoload.php';

	include_once('class.job-runner.php');
	include_once('class.json-validator.php');
	include_once('functions.php');

	/*
		$cfg = getopt("",["outdir:","repository:","logserver:","jobfolder:","datafolder:","outfile_lines:"]);

		if (is_null($cfg["outdir"]) || is_null($cfg["repository"]))
		{
			$t="usage:\n".
			     "sudo php process_datasets.php --outdir --repository [--jobfolder --datafolder] [--logserver]\n".
			     "  --outdir         target folder for output of processed files (`--outdir=/data/output/`)\n".
			     "  --repository     source folder with datasets (`--repository=/data/datasets/`)\n" .
			     "  --jobfolder      dataset target folder for next step in import chain\n" .
			     "                   (optional; if provided, --datafolder is also required)\n" .
			     "  --datafolder     datafile target folder for next step in import chain\n" .
			     "                   (optional; if provided, --jobfolder is also required)\n" .
			     "  --logserver      elasticsearch logsever adress (optional)\n" .
			     "  --outfile_lines  max length in lines validator output files (optional; default 500000; use 0 for all in one)\n"
			     ;
			     
			echo $t;
			exit(0);
		}
	*/

	$suppress_slack_posts = isset($_ENV["SLACK_ENABLED"]) ? $_ENV["SLACK_ENABLED"]==0 : false;
	$suppress_log_server_feedback = true;
	$slack_hook = isset($_ENV["SLACK_WEBHOOK"]) ? $_ENV["SLACK_WEBHOOK"] : null;
	$log_server_address = isset($_ENV["logserver"]) ? $_ENV["logserver"] : null;
	$preprocessor_job_folder = isset($_ENV["jobfolder"]) ? $_ENV["jobfolder"] : null;
	$preprocessor_file_folder = isset($_ENV["datafolder"]) ? $_ENV["datafolder"] : null;
	$outfile_lines = isset($_ENV["outfile_lines"]) ? intval($_ENV["outfile_lines"]) : 500000;

	// we're not outputting anything smaller than 50,000 lines (0 means all in one file)
	$outfile_lines = ($outfile_lines<50000 && $outfile_lines!=0) ? 500000 : $outfile_lines;

	// get preprocessing database data input dir
	// $outdir = getOutDir();
	$outdir = isset($_ENV["outdir"]) ? $_ENV["outdir"] : null;
	if (empty($outdir)) throw new Exception("no output directory specified");
	if (!file_exists($outdir)) throw new Exception(sprintf("output directory %s does not exist",$outdir));

	// read all datasets
	$repository = isset($_ENV["repository"]) ? $_ENV["repository"] : null;
	if (empty($repository)) throw new Exception("no repository path specified");
	if (!file_exists($repository)) throw new Exception(sprintf("repository directory %s does not exist",$repository));

	$datasets = readRepository( $repository );

	// keep only the ones with status 'pending'
	$jobs=[];
	foreach ($datasets as $dataset)
	{
		$t = json_decode(file_get_contents($dataset),true);
		if ($t["status"]=="pending")
		{
			$jobs[]=$t;
		}
	}

	//running jobs
	echo sprintf("found %s job(s)\n",count($jobs));

	foreach ($jobs as $job)
	{
		echo sprintf("reading job %s\n",$job["id"]);
		echo sprintf("job start: %s\n",date('c',time()));

		$time_pre = microtime(true);
		$job_runner = new jobRunner($job);
		$job_runner->setOutDir($outdir);
		$job_runner->setValidatorMaxOutfileLength($outfile_lines);

		try
		{
			echo sprintf("processing job %s\n",$job["id"]);
			$job["status"]="processing";
			storeUpdatedJobFile( $job );
			$job_runner->run();
			$status="validated";
		} 
		catch(Exception $e)
		{
			$status="failed validation";
			$status_info=$e->getMessage();
			echo sprintf("!!! ABORTING JOB %s: %s\n",$job["id"],$status_info);
		}

		// test run: delete not archive
		$job_runner->archiveOriginalFiles();

		$time_taken = secondsToTime(microtime(true) - $time_pre);

		$job = $job_runner->getJob();

		$job["status"]=$status;
		$job["validator_time_taken"]=$time_taken;

		if (isset($status_info))
		{
			$job["status_info"]=$status_info;
		}

		$job["validator_client_error_files"] = moveErrorFilesToReportDir($job);

		$reports=writeClientReport($job);
		$job["validator_client_reports"]=$reports;
		echo "wrote " , implode(";", $job["validator_client_reports"]) , "\n";

		storeUpdatedJobFile( $job );
		echo "wrote " , $job["dataset_filename"] , "\n";

		if (!$suppress_log_server_feedback)
		{
			if (!is_null($log_server_address))
			{
				putValidationLog( $job, $log_server_address );
				echo "put validation log to server" , "\n";
			}
			else
			{
				echo sprintf("error: log server address missing from ENV"), "\n";
			}
		}	
		else
		{
			echo sprintf("log server feedback suppressed"), "\n";
		}

		if ($job["status"]=="validated" && !is_null($preprocessor_job_folder) && !is_null($preprocessor_file_folder))
		{
			$job = moveJobFilesToPercolator( $job, $preprocessor_job_folder, $preprocessor_file_folder );	
			echo "moved to " , $job["dataset_filename"] , "\n";
		}
		else
		{
			$c=[];
			if ($job["status"]!="validated")
			{
				$c[]="job status != validated";
			}
			if (is_null($preprocessor_job_folder))
			{
				$c[]="preprocessor_job_folder is null";
			}
			if (is_null($preprocessor_file_folder))
			{
				$c[]="preprocessor_file_folder is null";
			}

			echo sprintf("skipped moving files to preprocessor (%s)",implode("; ", $c)), "\n";
		}

		storeUpdatedJobFile( $job );

		if (!$suppress_slack_posts)
		{
			if (!is_null($slack_hook))
			{
				postSlackJobResults( $slack_hook, $job );
			}
			else
			{
				echo sprintf("error: slack hook missing from ENV"), "\n";
			}
		}	
		else
		{
			echo sprintf("slack feedback suppressed"), "\n";
		}

		echo sprintf("finished job %s\n",$job["id"]);
		echo sprintf("job file: %s\n",$job["dataset_filename"]);
		echo "job took ", $time_taken, "\n\n";
	}
