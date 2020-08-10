<?php

	/*
		SUPPPLIER_CONFIG_FILE   // crs.ini *
		VALIDATOR_JOB_FOLDER    // stored jobs *
		FORCE_DATA_REPLACE      // 1 or 0 (or absent)
		TMP_FOLDER              // tmp path (defaults to system tmp)
	*/

    include_once("lib/class.dataSet.php");   
    include_once("lib/class.inputPrepare.php"); 
    include_once("lib/class.logClass.php");   
    include_once("lib/functions.php");

	try
	{
        $logFile = getenv("LOG_FILE");

	    $logger = new LogClass($logFile,"create dataset");

		if (empty(getenv('SUPPPLIER_CONFIG_FILE'))) throw new Exception("need a config file (env: SUPPPLIER_CONFIG_FILE)");

		$cfg_file = getenv('SUPPPLIER_CONFIG_FILE');
		$cfg = parse_ini_file($cfg_file,true,INI_SCANNER_TYPED);

		if (!$cfg) throw new Exception(sprintf("can't read config file %s",$cfg_file));

        $repoPath = realpath(getenv("VALIDATOR_JOB_FOLDER"));
        $tmpPath = realpath(getenv("TMP_FOLDER"));

		if (empty($repoPath)) throw new Exception("no validator job folder specified (env: VALIDATOR_JOB_FOLDER)");

		$force_data_replace = getenv('FORCE_DATA_REPLACE') ? getenv('FORCE_DATA_REPLACE')=='1' : false;

		$logger->info(sprintf("config: %s", $cfg_file));
		$logger->info(sprintf("repo: %s",$repoPath));
		$logger->info(sprintf("tabula rasa: %s",( $force_data_replace ? "y" : "n" )));

		# inputPrepare: unpacks archives, renames files to valid extensions
		$p = new inputPrepare;
		$p->setLogClass($logger);

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

		$d->setLogClass($logger);
		$d->setChangedNames($changes);
		$d->setForceDataReplace($force_data_replace);
		$d->setDataSupplierCode($cfg["supplier_codes"]["source_system_code"]);

		if (isset($cfg["specimen"]) && $cfg["specimen"]["input_dir"])
		{
			$d->addInputDirectory($cfg["specimen"]["input_dir"],"specimen");
			$logger->info(sprintf("added %s (%s)",$cfg["specimen"]["input_dir"],"specimen"));
			$d->setIsIncremental($cfg["specimen"]["is_incremental"],"specimen");
		}
		
		if (isset($cfg["multimedia"]) && $cfg["multimedia"]["input_dir"])
		{
			$d->addInputDirectory($cfg["multimedia"]["input_dir"],"multimedia");
			$logger->info(sprintf("added %s (%s)",$cfg["multimedia"]["input_dir"],"multimedia"));
			$d->setIsIncremental($cfg["multimedia"]["is_incremental"],"multimedia");
		}
		
		if (isset($cfg["taxon"]) && $cfg["taxon"]["input_dir"])
		{
			$d->addInputDirectory($cfg["taxon"]["input_dir"],"taxon");
			$logger->info(sprintf("added %s (%s)",$cfg["taxon"]["input_dir"],"taxon"));
			$d->setIsIncremental($cfg["taxon"]["is_incremental"],"taxon");
		}

		if (isset($cfg["geo"]) && $cfg["geo"]["input_dir"])
		{
			$d->addInputDirectory($cfg["geo"]["input_dir"],"geo");
			$logger->info(sprintf("added %s (%s)",$cfg["geo"]["input_dir"],"geo"));
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

		$logger->info(sprintf("wrote %s",$dataset_filename));
		$d->removeProcessingFlags();
		exit(0);

	} 
	catch(Exception $e)
	{
        $logger->error($e->getMessage() . "; exiting");
		exit(1);
	}
