<?php

	require __DIR__ . '/vendor/autoload.php';

	include_once('functions.php');
	include_once('class.file-slicer.php');

	// $preprocessor_job_folder = isset($_ENV["jobfolder"]) ? $_ENV["jobfolder"] : null;
	// $preprocessor_file_folder = isset($_ENV["datafolder"]) ? $_ENV["datafolder"] : null;
	// $repository = isset($_ENV["repository"]) ? $_ENV["repository"] : null;
	$preprocessor_job_folder = !empty(getenv("jobfolder")) ? getenv("jobfolder") : null;
	$preprocessor_file_folder = !empty(getenv("datafolder")) ? getenv("datafolder") : null;
	$repository = !empty(getenv("repository")) ? getenv("repository") : null;
	$parallel_processes = !empty(getenv("parallel_processes")) ? getenv("parallel_processes") : 6;

	if (empty($repository)) throw new Exception("no repository path specified");
	if (!file_exists($repository)) throw new Exception(sprintf("repository directory %s does not exist",$repository));

	$datasets = readRepository( $repository );

	$jobs=[];
	foreach ($datasets as $dataset)
	{
		$t = json_decode(file_get_contents($dataset),true);

		if ($t["status"]=="pending" && $t["use_parallel_processing"]==true)
		{
			$t["status"]="splicing";
			storeUpdatedJobFile( $t );
			$jobs[]=$t;
		}
	}

	echo sprintf("number of parallel processes: %s\n",$parallel_processes);
	echo sprintf("found %s sliceable job(s)\n",count($jobs));

	$splicer = new FileSlicer();
	$splicer->setNumberOfParallelProcesses( $parallel_processes );

	foreach ($jobs as $job)
	{
		echo sprintf("slicing job %s\n",$job["id"]);

		try
		{
			unset($job["slicing"]);

			$slice_id = substr(sha1(microtime() . getmypid()),-7);

			$job["slicing"]["status"]="pending";
			$job["slicing"]["id"]=$slice_id;
			$job["slicing"]["file_add"]="[slice_" . $slice_id . "-%s]";

			echo sprintf("slice id: %s\n",$slice_id,);
			
			foreach($job["input"] as $type=>$files)
			{
				$splicer->reset();

				foreach ($files as $file)
				{
					$splicer->addFile( $file["tmp_path"], $file["path"], isset($file["lines"]) ? $file["lines"] : 0 );
				}

				$splicer->calculateSplit();
				$job["slicing"]["input"][$type]=$splicer->getSlices();
				$t = $splicer->getOverallNumbers();
				echo sprintf("%s: sliced %s lines into %s slices of max %s lines\n",
					$type,
					number_format($t["total_lines"]),
					$t["slices"],
					number_format($t["slice_size"])
				);
			}

			$job["status"]="pending";
			
			storeUpdatedJobFile( $job );
		} 
		catch(Exception $e)
		{
			echo sprintf("aborting slicing for %s: %s\n",$job["id"],$e->getMessage());
			$job["status"]="pending";
			$job["slicing"]["status"]="failed";
			unset($job["slicing"]["input"]);
			storeUpdatedJobFile( $job );
		}			
	}
