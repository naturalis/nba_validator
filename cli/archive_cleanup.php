<?php

	include("class.dataset.php");	
	include('functions.php');

	printArchiveCleanupUsage();

	define("NUMBER_OF_ARCHIVES_TO_KEEP", 2);

	$archivePath = realpath(@getopt("",["archive:"])["archive"]);
	echo "archive: " , $archivePath , "\n";
	echo "NUMBER_OF_ARCHIVES_TO_KEEP: ", NUMBER_OF_ARCHIVES_TO_KEEP, "\n";

	$archives=[];
	foreach(glob($archivePath . "/*.tar.gz" ) as $file) {
		$boom = explode(".",basename($file));
		$archives[$boom[0]][$boom[1]]=$file;
	}

	foreach ($archives as $key => $a) {
		krsort($archives[$key]);
	}

	print_r($archives);

	foreach ($archives as $key => $a) {
		$l=0;
		foreach ($a as $archive) {
			if ($l>=NUMBER_OF_ARCHIVES_TO_KEEP) {
				unlink($archive);
				echo sprintf("deleted %s",basename($archive)) , "\n";
			}
			$l++;
		}
	}
