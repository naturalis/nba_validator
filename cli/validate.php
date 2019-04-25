<?php

 	// php validate.php --outdir=/tmp/ --schema=/opt/git/schemas/specimen--v2.17.json --file=/data/infile.jsonl 

	require __DIR__ . '/vendor/autoload.php';

	include_once('class.json-validator.php');

	$cfg = getopt("",["outdir:","schema:","file:","merge:"]);

	if (is_null($cfg["outdir"]) || is_null($cfg["schema"]) || is_null($cfg["file"]))
	{
		$t[]="usage:";
		$t[]="sudo php validate.php --outdir=/outdir/ --schema=schema.json --file=<infile.jsonl | /indir/ > [--merge=extra_schema.json]";
		echo implode("\n",$t) . "\n";
		exit(0);
	}

	$validator = new JsonValidator([
		'output_dir' =>	$cfg["outdir"],
		'schema_file' => $cfg["schema"]
	]);

	if (isset($cfg["merge"]))
	{
		$validator->setAdditionalJsonSchema($cfg["merge"]);
	}

	$validator->setAllowDoubleIds(false);
	$validator->setFailOnAnyError(false);
	$validator->setUseISO8601DateCheck(false);
	$validator->setMaxOutfileLength(500000);

	if (is_dir($cfg["file"]))
	{
		foreach (glob(rtrim($cfg["file"],"/") . "/*.jsonl") as $filename)
		{
		    $validator->addFileToValidate($filename);
		}
	}
	else
	{
		$validator->addFileToValidate($cfg["file"]);
	}

	print_r($validator->getSettingsOverview());

	$validator->run();

	print_r($validator->getValidationOverview());

