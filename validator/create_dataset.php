<?php

	/*

		php create_dataset.php --config=/config/config.ini --force_data_replace

		$_ENV["repository"];
		$_ENV["tmp_path"]; (optional)

	*/

	include("class.dataset.php");	
	include("class.input-prepare.php");	
	include('functions.php');

	$cfg = getopt("",["config:"]);

	try
	{
		$force_data_replace = array_key_exists("force_data_replace",@getopt("",["force_data_replace"]));
		$use_parallel_processing = array_key_exists("parallel_processing",@getopt("",["parallel_processing"]));

		$repoPath = getenv("repository");
		$tmpPath = getenv("tmp_path");

		if (empty($repoPath)) throw new Exception("no repo path specified");

		$cfg = readConfig();
		$supplierConfigFile = getConfigFile();

		echo "config: " , $supplierConfigFile , "\n";
		echo "repo: " , $repoPath , "\n";
		echo "tabula rasa: " , ( $force_data_replace ? "y" : "n" ) , "\n";
		echo "parallel processing: " , ( $use_parallel_processing ? "y" : "n" ) , "\n";

		# inputPrepare: unpacks archives, renames files to valid extensions
		$p = new inputPrepare;

		if (isset($cfg["specimen"]) && $cfg["specimen"]["input_dir"] && file_exists($cfg["specimen"]["input_dir"]))
		{
			$p->addDirToPrepare($cfg["specimen"]["input_dir"]);
		}

		if (isset($cfg["multimedia"]) && $cfg["multimedia"]["input_dir"] && file_exists($cfg["multimedia"]["input_dir"]))
		{
			$p->addDirToPrepare($cfg["multimedia"]["input_dir"]);
		}

		if (isset($cfg["taxon"]) && $cfg["taxon"]["input_dir"] && file_exists($cfg["taxon"]["input_dir"]))
		{
			$p->addDirToPrepare($cfg["taxon"]["input_dir"]);
		}

		if (isset($cfg["geo"]) && $cfg["geo"]["input_dir"] && file_exists($cfg["geo"]["input_dir"]))
		{
			$p->addDirToPrepare($cfg["geo"]["input_dir"]);
		}

		$p->run();
		$changes = $p->getNameChanges();

		$d = new dataSet;
		$d->setChangedNames($changes);
		$d->setForceDataReplace($force_data_replace);
		$d->setParallelProcessing($use_parallel_processing);
		$d->setDataSupplierCode($cfg["supplier_codes"]["source_system_code"]);
		
		if (isset($cfg["specimen"]) && $cfg["specimen"]["input_dir"])
		{
			$d->addInputDirectory($cfg["specimen"]["input_dir"],"specimen");
			echo sprintf("added %s (%s)\n",$cfg["specimen"]["input_dir"],"specimen");
			$d->setIsIncremental($cfg["specimen"]["is_incremental"],"specimen");
		}
		
		if (isset($cfg["multimedia"]) && $cfg["multimedia"]["input_dir"])
		{
			$d->addInputDirectory($cfg["multimedia"]["input_dir"],"multimedia");
			echo sprintf("added %s (%s)\n",$cfg["multimedia"]["input_dir"],"multimedia");
			$d->setIsIncremental($cfg["multimedia"]["is_incremental"],"multimedia");
		}
		
		if (isset($cfg["taxon"]) && $cfg["taxon"]["input_dir"])
		{
			$d->addInputDirectory($cfg["taxon"]["input_dir"],"taxon");
			echo sprintf("added %s (%s)\n",$cfg["taxon"]["input_dir"],"taxon");
			$d->setIsIncremental($cfg["taxon"]["is_incremental"],"taxon");
		}

		if (isset($cfg["geo"]) && $cfg["geo"]["input_dir"])
		{
			$d->addInputDirectory($cfg["geo"]["input_dir"],"geo");
			echo sprintf("added %s (%s)\n",$cfg["geo"]["input_dir"],"geo");
			$d->setIsIncremental($cfg["geo"]["is_incremental"],"geo");
		}

		$d->setSupplierConfigFile($supplierConfigFile);
		$d->setOutputDirectory($repoPath);
		$d->setReportDirectory($cfg["settings"]["report_dir"]);

		if (!empty($tmpPath))
		{
			$d->setTmpDirectory($tmpPath);
		}

		$d->makeDataset();
		$d->prepareSetForProcessing();

		$dataset_filename = $d->getDatasetFilename();

		echo sprintf("wrote %s\n",$dataset_filename);
		$d->removeProcessingFlags();
		exit(0);

	} 
	catch(Exception $e)
	{
		print("error: " . $e->getMessage() . "\n");
		// print("* " . implode("\n* ",$d->getMessages()) . "\n");
		exit(1);
	}
