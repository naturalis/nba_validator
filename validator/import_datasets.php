<?php

    include_once("lib/class.dataSet.php");   
    include_once("lib/class.inputPrepare.php"); 
    include_once("lib/class.logClass.php");   
    include_once("lib/functions.php");

    $processableStatus="IN_PROGRESS";

    try
    {
        $jobPath = realpath(getenv("INCOMING_JOB_FOLDER"));
        $cfgPath = realpath(getenv("INI_FILE_FOLDER"));
        $repoPath = realpath(getenv("VALIDATOR_JOB_FOLDER"));
        $tmpPath = realpath(getenv("TMP_FOLDER"));
        $logFile = getenv("LOG_FILE");

        $logger = new LogClass($logFile,"import datasets");
        $dataset_logger = new LogClass($logFile,"dataset");
        $input_logger = new LogClass($logFile,"input prepare");

        $cfgFiles = json_decode(getenv("INI_FILE_LIST"),true);

        if (empty($jobPath)) throw new Exception("no incoming job folder (env: INCOMING_JOB_FOLDER)");
        if (empty($cfgPath)) throw new Exception("no validator configurations path specified (env: INI_FILE_FOLDER)");
        if (empty($repoPath)) throw new Exception("no validator job folder specified (env: VALIDATOR_JOB_FOLDER)");
        if (!file_exists($jobPath)) throw new Exception(sprintf("job path doesn't exist: %s",$jobPath));
        if (!file_exists($cfgPath)) throw new Exception(sprintf("validator configurations path doesn't exist: %s",$cfgPath));
        if (empty($cfgFiles)) throw new Exception("missing or malformed ini file list (env: INI_FILE_LIST)");

        $jobs=[];

        // reading jobfiles
        $jobFiles = glob($jobPath."/*.json");
        foreach ($jobFiles as $jobFile)
        {
            $jobs[$jobFile]=json_decode(file_get_contents($jobFile),true);
        }

        //running jobs
        $logger->info(sprintf("found %s job(s)",count($jobs)));

        foreach ($jobs as $thisJobFile => $job)
        {
            $logger->info(sprintf("reading job file %s",$thisJobFile));

            if (!isset($job["status"]) || $job["status"]!=$processableStatus)
            {
                $logger->info(sprintf("skipping jobfile %s (status should be '%s', but is '%s')", 
                    $thisJobFile, $processableStatus, @$job["status"]));
                continue;
            }

            $base_subdirs=[];

            if (array_key_exists($job["data_supplier"], $cfgFiles))
            {
                $cfgFile = $cfgPath . "/" . $cfgFiles[$job["data_supplier"]];

                if (!file_exists($cfgFile)) throw new Exception(sprintf("ini file doesn't exist: %s",$cfgFile));

                $cfg = parse_ini_file($cfgFile,true,INI_SCANNER_TYPED);

                $logger->info(sprintf("config: %s", $cfgFile));

                $src = $job["source_directories"];

                foreach ($src as $val)
                {
                    $base_subdirs[] = pathinfo($val)["dirname"];
                }

                $jobPath_specimen = isset($src["specimen"]) ? $jobPath . "/" . $src["specimen"] : null;
                $jobPath_multimedia = isset($src["multimedia"]) ? $jobPath . "/" . $src["multimedia"] : null;
                $jobPath_taxon = isset($src["taxon"]) ? $jobPath . "/" . $src["taxon"] : null;
                $jobPath_geo = isset($src["geo"]) ? $jobPath . "/" . $src["geo"] : null;

                # inputPrepare: unpacks archives, renames files to valid extensions
                $p = new inputPrepare;

                $p->setLogClass($input_logger);

                if (isset($cfg["specimen"]) && isset($jobPath_specimen))
                {
                    if (!file_exists($jobPath_specimen))
                    {
                        $logger->error(sprintf("folder doesn't exist: %s",$jobPath_specimen));
                        unset($jobPath_specimen);
                    }
                    else
                    {
                        $p->addDirToPrepare($jobPath_specimen);
                    }
                }

                if (isset($cfg["multimedia"]) && isset($jobPath_multimedia))
                {
                    if (!file_exists($jobPath_multimedia))
                    {
                        $logger->error(sprintf("folder doesn't exist: %s",$jobPath_multimedia));
                        unset($jobPath_multimedia);
                    }
                    else
                    {
                        $p->addDirToPrepare($jobPath_multimedia);
                    }
                }

                if (isset($cfg["taxon"]) && isset($jobPath_taxon))
                {
                    if (!file_exists($jobPath_taxon))
                    {
                        $logger->error(sprintf("folder doesn't exist: %s",$jobPath_taxon));
                        unset($jobPath_taxon);
                    }
                    else
                    {
                        $p->addDirToPrepare($jobPath_taxon);
                    }
                }

                if (isset($cfg["geo"]) && isset($jobPath_geo))
                {
                    if (!file_exists($jobPath_geo))
                    {
                        $logger->error(sprintf("folder doesn't exist: %s",$jobPath_geo));
                        unset($jobPath_geo);
                    }
                    else
                    {
                        $p->addDirToPrepare($jobPath_geo);                    
                    }
                }

                $p->run();
                $changes = $p->getNameChanges();

                $d = new dataSet;
              
                $d->setLogClass($dataset_logger);
                $d->setChangedNames($changes);
                $d->setForceDataReplace($job["tabula_rasa"]);
                $d->setImportedDataset(true);
                $d->setDataSupplierCode($cfg["supplier_codes"]["source_system_code"]);

                if (isset($cfg["specimen"]) && isset($jobPath_specimen))
                {
                    $d->addInputDirectory($jobPath_specimen,"specimen");
                    $logger->info(sprintf("added %s (%s)",$jobPath_specimen,"specimen"));
                    $d->setIsIncremental($cfg["specimen"]["is_incremental"],"specimen");
                }
                
                if (isset($cfg["multimedia"]) && isset($jobPath_multimedia))
                {
                    $d->addInputDirectory($jobPath_multimedia,"multimedia");
                    $logger->info(sprintf("added %s (%s)",$jobPath_multimedia,"multimedia"));
                    $d->setIsIncremental($cfg["multimedia"]["is_incremental"],"multimedia");
                }
                
                if (isset($cfg["taxon"]) && isset($jobPath_taxon))
                {
                    $d->addInputDirectory($jobPath_taxon,"taxon");
                    $logger->info(sprintf("added %s (%s)",$jobPath_taxon,"taxon"));
                    $d->setIsIncremental($cfg["taxon"]["is_incremental"],"taxon");
                }

                if (isset($cfg["geo"]) && isset($jobPath_geo))
                {
                    $d->addInputDirectory($jobPath_geo,"geo");
                    $logger->info(sprintf("added %s (%s)",$jobPath_geo,"geo"));
                    $d->setIsIncremental($cfg["geo"]["is_incremental"],"geo");
                }

                $d->setSupplierConfigFile($cfgFile);
                $d->setOutputDirectory($repoPath);
                $d->setReportDirectory($cfg["settings"]["report_dir"]);

                if (!empty($tmpPath))
                {
                    $d->setTmpDirectory($tmpPath);
                }

                if (isset($job["id"]))
                {
                    $d->setJobIdOverride($job["id"]);
                }
                else
                {
                    $logger->warning(sprintf("job file has no id; creating new id"));
                }

                if (isset($job["date"]))
                {
                    $d->setJobDateOverride($job["date"]);
                }
                else
                {
                    $logger->warning(sprintf("job file has no date; creating new date"));
                }

                $d->setIsTestRun($job["test_run"] ?? false);
                $d->addInheritedMetadata("job_creator", ["source_directories" => $job["source_directories"]]);
                $d->makeDataset();
                $d->prepareSetForProcessing();

                $dataset_filename = $d->getDatasetFilename();

                $logger->info(sprintf("wrote %s",$dataset_filename));

                $d->removeProcessingFlags();
               
                foreach (array_unique($base_subdirs) as $base_subdir)
                {
                    $this_dir = $jobPath . "/" . $base_subdir;
                    $logger->info(sprintf("deleting temporary source dir %s",$this_dir));
                    rmDirRecursive($this_dir);
                }

                unlink($thisJobFile);

                $logger->info(sprintf("job '%s' done",$job["id"]));
            }
            else
            {
                $logger->error(sprintf("unknown data supplier code %s",$job["data_supplier"]));
            }
        }

        exit(0);

    } 
    catch(Exception $e)
    {
        $logger->error($e->getMessage());
        exit(1);
    }
