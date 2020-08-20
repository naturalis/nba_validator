<?php

/*
	VALIDATOR_JOB_FOLDER
	VALIDATED_OUTPUT_FOLDER
	OUTGOING_JOB_FOLDER
	OUTGOING_OUTPUT_FOLDER	
	OUTGOING_TEST_JOB_FOLDER
*/

	require __DIR__ . '/vendor/autoload.php';

	include_once("lib/class.jobRunner.php");
	include_once("lib/class.jsonValidator.php");
    include_once("lib/class.logClass.php");   
	include_once("lib/functions.php");

    $logFile = getenv("LOG_FILE");
    $logger = new LogClass($logFile,"process datasets");

	$incoming_job_files = getenv('VALIDATOR_JOB_FOLDER');
	$validated_output_folder = getenv('VALIDATED_OUTPUT_FOLDER');
	$outgoing_job_folder = getenv('OUTGOING_JOB_FOLDER');
	$outgoing_output_folder = getenv('OUTGOING_OUTPUT_FOLDER');
	$outgoing_test_job_folder = getenv('OUTGOING_TEST_JOB_FOLDER');

	$suppress_slack_posts = getenv("SLACK_ENABLED") ? getenv("SLACK_ENABLED")==0 : false;
	$slack_hook = getenv("SLACK_WEBHOOK");
	$outfile_lines = getenv("OUTFILE_LINES") ? intval(getenv("OUTFILE_LINES")) : 500000;
	$outfile_lines = ($outfile_lines<50000 && $outfile_lines!=0) ? 500000 : $outfile_lines;

	try
	{
		foreach ([
			"VALIDATOR_JOB_FOLDER" => $incoming_job_files,
			"VALIDATED_OUTPUT_FOLDER" => $validated_output_folder,
			"OUTGOING_JOB_FOLDER" => $outgoing_job_folder,
			"OUTGOING_OUTPUT_FOLDER" => $outgoing_output_folder,
			"OUTGOING_TEST_JOB_FOLDER" => $outgoing_test_job_folder
		] as $key => $value)
		{
			if (empty($value)) throw new Exception(sprintf("%s not specified",$key));
			if (!file_exists($value)) throw new Exception(sprintf("%s '%s' doesn't exist",$key,$value));
		}
	}
	catch(Exception $e)
	{
		$logger->error(sprintf("aborting: %s",$e->getMessage()));
		exit(0);
	}

	// read available datasets
	$datasets = glob(rtrim($incoming_job_files,"/") . "/*.json");
	usort($datasets, create_function('$a,$b', 'return filemtime($a) - filemtime($b);'));

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
	$logger->info(sprintf("found %s job(s)",count($jobs)));

	foreach ($jobs as $job)
	{
		$logger->info(sprintf("reading job %s",$job["id"]));
		$logger->info(sprintf("job start: %s",date('c',time())));

		$time_pre = microtime(true);

		$job_runner = new JobRunner($job);
		$job_logger = new LogClass("log/validator.log","job runner");
		$job_runner->setLogClass($job_logger);
		$job_runner->setOutputDir($validated_output_folder);
		$job_runner->setValidatorMaxOutfileLength($outfile_lines);

		try
		{
			$logger->info(sprintf("processing job %s",$job["id"]));
			$job["status"]="processing";
			$job_runner->storeJobFile( $job );
			$job_runner->run();
			$status="validated";
		} 
		catch(Exception $e)
		{
			$status="failed validation";
			$status_info=$e->getMessage();
			$logger->error(sprintf("ABORTING JOB %s: %s",$job["id"],$status_info));
		}

		$job_runner->archiveValidationFiles();

		$time_taken = secondsToTime(microtime(true) - $time_pre);

		$job = $job_runner->getJob();

		$job["status"]=$status;
		$job["validator_time_taken"]=$time_taken;

		if (isset($status_info))
		{
			$job["status_info"]=$status_info;
		}

		$job_runner->storeJobFile( $job );
		$job_runner->moveErrorFilesToReportDir();
		$job_runner->writeClientReport();

		if (!$job["test_run"])
		{
			if ($job["status"]=="validated")
			{
				$job_runner->moveValidatedFiles( $outgoing_output_folder );	
				$job_runner->moveJobFile( $outgoing_job_folder );	
			}
			else
			{
				$logger->info(sprintf("skipped moving files: job status = %s",$job["status"]));
			}
		}
		else
		{
			$job_runner->deleteDataFiles();
			$job_runner->moveJobFile( $outgoing_test_job_folder );
		}

		if (!$suppress_slack_posts && !is_null($slack_hook))
		{
			postSlackJobResults( $slack_hook, $job );
		}	

		$logger->info(sprintf("finished job %s",$job["id"]));
		$logger->info(sprintf("job file: %s",$job["dataset_filename"]));
		$logger->info(sprintf("job took %s", $time_taken));
	}
