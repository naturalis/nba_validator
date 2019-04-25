<?php

	require __DIR__ . '/vendor/autoload.php';

	include_once('class.json-validator.php');

	$cfg = getopt("",["file:","schema:","merge:"]);
	if (is_null($cfg["file"] ) || is_null($cfg["schema"]))
	{
		$t[]="usage:";
		$t[]="sudo php validate_single.php --file=/path/to/json-file --schema=/path/to/json-schema [--merge=/path/to/extra_schema.json]";
		echo implode("\n",$t) . "\n";
		exit(0);
	}

	$file = isset($cfg["file"]) ? $cfg["file"] : null;
	$schema = isset($cfg["schema"]) ? $cfg["schema"] : null;

	$validator = new JsonValidator([
		'output_dir' =>	"/tmp/",
		'schema_file' => $schema
	]);

	if (isset($cfg["merge"]))
	{
		$validator->setAdditionalJsonSchema($cfg["merge"]);
	}

	$validator->setUseISO8601DateCheck(true);
	$validator->validateRawJsonDoc(file_get_contents($file));
