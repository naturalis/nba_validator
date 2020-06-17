<?php

/*
    // new
    getenv("job_path")  // incoming jobs
    getenv("cfg_path")  // validator ini-file folder

    // existing
    getenv("repository");
    getenv("tmp_path");

*/


    include("class.dataset.php");   
    include("class.input-prepare.php"); 
    include('functions.php');

    $supplierInifFiles = 
        [
            "BRAHMS" => "brahms.ini",
            "COL" => "col.ini",
            "CRS" => "crs.ini",
            "CSR" => "csr.ini",
            "GEO" => "geoareas.ini",
            "NSR" => "nsr.ini",
            "OBS" => "obs.ini",
            "XC" => "xenocanto.ini"
        ];

    $processableStatus="IN_PROGRESS";

    try
    {
        $jobPath = realpath(getenv("job_path"));
        $cfgPath = realpath(getenv("cfg_path"));

        $repoPath = getenv("repository");
        $tmpPath = getenv("tmp_path");

        if (empty($jobPath)) throw new Exception("no job path specified (env: job_path)");
        if (empty($cfgPath)) throw new Exception("no validator configurations path specified (env: cfg_path)");
        if (empty($repoPath)) throw new Exception("no repo path specified");


        if (!file_exists($jobPath)) throw new Exception(sprintf("job path %s doesn't exist",$jobPath));
        if (!file_exists($cfgPath)) throw new Exception(sprintf("validator configurations path %s doesn't exist",$cfgPath));

        $jobs=[];

        // reading jobfiles
        $jobFiles = glob($jobPath."/*.json");
        foreach ($jobFiles as $jobFile)
        {
            $jobs[$jobFile]=json_decode(file_get_contents($jobFile),true);
        }

        foreach ($jobs as $thisJobFile => $job)
        {

            echo sprintf("reading job file %s\n",$thisJobFile);

            if (!isset($job["status"]) || $job["status"]!=$processableStatus)
            {
                echo sprintf("skipping jobfile %s (status should be '%s', but is '%s')\n", $thisJobFile, $processableStatus, @$job["status"]);
                continue;
            }

            $base_subdirs=[];

            if (array_key_exists($job["data_supplier"], $supplierInifFiles))
            {

                $supplierConfigFile = $cfgPath . "/" . $supplierInifFiles[$job["data_supplier"]];
                $cfg = readConfig($supplierConfigFile);

                echo sprintf("config: %s\n", $supplierConfigFile);

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

                if (isset($cfg["specimen"]) && isset($jobPath_specimen))
                {
                    if (!file_exists($jobPath_specimen))
                    {
                        echo sprintf("folder doesn't exist: %s\n",$jobPath_specimen);
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
                        echo sprintf("folder doesn't exist: %s\n",$jobPath_multimedia);
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
                        echo sprintf("folder doesn't exist: %s\n",$jobPath_taxon);
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
                        echo sprintf("folder doesn't exist: %s\n",$jobPath_geo);
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

                $d->setChangedNames($changes);
                $d->setForceDataReplace($job["tabula_rasa"]);
                $d->setDataSupplierCode($cfg["supplier_codes"]["source_system_code"]);

                if (isset($cfg["specimen"]) && isset($jobPath_specimen))
                {
                    $d->addInputDirectory($jobPath_specimen,"specimen");
                    echo sprintf("added %s (%s)\n",$jobPath_specimen,"specimen");
                    $d->setIsIncremental($cfg["specimen"]["is_incremental"],"specimen");
                }
                
                if (isset($cfg["multimedia"]) && isset($jobPath_multimedia))
                {
                    $d->addInputDirectory($jobPath_multimedia,"multimedia");
                    echo sprintf("added %s (%s)\n",$jobPath_multimedia,"multimedia");
                    $d->setIsIncremental($cfg["multimedia"]["is_incremental"],"multimedia");
                }
                
                if (isset($cfg["taxon"]) && isset($jobPath_taxon))
                {
                    $d->addInputDirectory($jobPath_taxon,"taxon");
                    echo sprintf("added %s (%s)\n",$jobPath_taxon,"taxon");
                    $d->setIsIncremental($cfg["taxon"]["is_incremental"],"taxon");
                }

                if (isset($cfg["geo"]) && isset($jobPath_geo))
                {
                    $d->addInputDirectory($jobPath_geo,"geo");
                    echo sprintf("added %s (%s)\n",$jobPath_geo,"geo");
                    $d->setIsIncremental($cfg["geo"]["is_incremental"],"geo");
                }

                $d->setSupplierConfigFile($supplierConfigFile);
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
                    echo sprintf("job file has no id; creating new id\n");
                }

                if (isset($job["date"]))
                {
                    $d->setJobDateOverride($job["date"]);
                }
                else
                {
                    echo sprintf("job file has no date; creating new date\n");
                }

                $d->setIsTestRun($job["test_run"] ?? false);

                $d->addInheritedMetadata("job_creator", ["source_directories" => $job["source_directories"]]);

                $d->makeDataset();

                $d->prepareSetForProcessing();

                $dataset_filename = $d->getDatasetFilename();

                echo sprintf("wrote %s\n",$dataset_filename);

                $d->removeProcessingFlags();
               
                foreach (array_unique($base_subdirs) as $base_subdir)
                {
                    $this_dir = $jobPath . "/" . $base_subdir;
                    echo sprintf("deleting temporary source dir %s\n",$this_dir);
                    rmDirRecursive($this_dir);
                }

                unlink($thisJobFile);

                echo "done","\n\n";
            }
            else
            {
                echo sprintf("unknown data supplier code %s\n",$job["data_supplier"]);
            }
        }

        exit(0);

    } 
    catch(Exception $e)
    {
        print("error: " . $e->getMessage() . "\n");
        // print("* " . implode("\n* ",$d->getMessages()) . "\n");
        exit(1);
    }
