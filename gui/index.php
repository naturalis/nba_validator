<?php

    $errors=null;
    $docs=[];
    $raw="";
    $schema="";

    $schemas = [
		"specimen/specimen--v2.17.json" => "specimen v2.17",
		"multimedia/multimedia--v2.17.json" => "multimedia v2.17",
		"taxon/taxon--v2.17.json" => "taxon v2.17"
    ];

    if (isset($_POST["raw"]) && !empty(trim($_POST["raw"])) && isset($_POST["schema"])) {

	    $raw=trim($_POST["raw"]);
	    $res=json_decode($raw);
	    $schema=$_POST["schema"];

	    // doesn't load as a single valid JSON-document, try as multi-line
        if (is_null($res)) {
			$praw=str_replace(["\r\n","\n\r","\r","\n"], PHP_EOL, $raw);
			$praw=explode(PHP_EOL, $praw);
			foreach ($praw as $key => $val) {
				if (empty(trim($val))) {
					continue;
				}
				$res=json_decode($val);
				if (is_null($res)) {
					$errors[$key][]=["parse_error" => "line $key unparsable (invalid JSON)"];
				}
				else {
					$docs[$key]=$val;
				}
			}

			// still no valid docs, might be a single broken doc after all, anyway single error will do
			if (count($docs)==0) {
				$errors=[];
				$errors[][]=["parse_error" => "unparsable document(s) (invalid JSON)"];
			}
        }
        else {
                $docs[]=$raw;
        }

        if (count($docs)>0) {

       		$git_root = dirname(__DIR__).'/../';
            require realpath(dirname($_SERVER["SCRIPT_FILENAME"])) . '/vendor/autoload.php';
            include_once($git_root . 'tools/validator/PHP/class.json-validator.php');

            $schema_file=$git_root . 'general/'.$schema;

            $v = new JsonValidator([
                'input_dir'=>'/tmp/',
                'output_dir'=>'/tmp/',
                'archive_dir'=>'/tmp/',
                'schema_file'=>$schema_file
            ]);

            $v->setSilent(true);
            $v->setSourceSystemDefaults(false);
            //$v->setIdElementName('unitID');

            foreach ($docs as $key => $value) {
	            $v->validateRawJsonDoc($value,$key);
	            $e = $v->getErrors();
	            if (!empty($e)) {
					$errors[$key] = $e;
	            }
            }
        }

        if (!empty($errors)) ksort($errors);

    }

?>
<html>

<script
  src="https://code.jquery.com/jquery-3.3.1.min.js"
  integrity="sha256-FgpCb/KJQlLNfOu91ta32o/NMZxltwRo8QtmkMRdAu8="
  crossorigin="anonymous"></script>

<style>
body {
	background-color:#fbfbfb;
}
p {
	margin-top:0px;
}
ul {
	list-style-type: none;
	margin: 0;
	padding: 0;
}
textarea {
	width: 100%;
	height: 400px;
	font-size:10.5px;
	font-family: Consolas;
}
#input {
    float: left;
    margin-right: 10px;
    width: 65%;
}
#howto {
    float: left;
    width: 30%;
	font-size:12px;
}
#clear {
	display:inline-block;
	margin-left:10px;
}
.errors.validate {
	display:inline-block;
	margin-bottom:10px;
}
.errors.schema {
	text-decoration:underline;
}
.errors.header {
	display:inline-block;
	margin-bottom:10px;
}
.errors.document {
	font-weight:bold;
}
.errors.error {
	margin-left: 20px;
}
.errors.key {
	margin-left: 40px;
	font-style: italic;
}
.errors.divider {
	display:inline-block;
	height:5px;
}
</style>
<body>
	<h1>Naturalis Biodiversity API JSON validator</h1>
		<div>
			<div id="input">
				<form method="post">
					<p>
						<textarea name="raw" id="textarea" placeholder="paste JSON" maxlength="100000"><?php echo htmlentities($raw); ?></textarea>
					</p>
					<p>
						validation schema: <select name="schema">
<?php
						foreach ($schemas as $key => $value) {
							print '<option value="'.$key.'"'.($key==$schema?" selected":"").'>'.$value.'</option>';
						}
?>
						</select>
					</p>
				<p>
					<input type="submit" name="validate" value="validate">
					<a id="clear" href="#" onclick="$('#textarea').val('');$('#errors').html('');$('#textarea').focus();return false;">clear</a>
				</p>
			</form>
		</div>
		<div id="howto">
			<p>
				to validate a JSON document, paste it in the textarea, select the correct validation schema and click 'validate'. you can validate a single document by pasting it "as is", including indenting spaces and line endings.
			</p>
			<p>
				if you want validate multiple JSON-documents at the same time, you can do so by pasting them all in at once, <i>one document per line</i>. make sure they are separate documents, not an array of documents (so no comma's at the line end, nor [ and ] at the start and end of the data). multiple documents should all be of the same type (specimen, taxon or multimedia).
			</p>
			<p>
				this validator is designed to verify the contents of your document against a NBA validation scheme (a link to the repo with the complete schemas is below). it is not suited for the verification of valid JSON documents per se (although it will raise an error if your document contains invalid JSON). for checking the validity of the JSON itself, use an <a href="https://jsoneditoronline.org" target="_blank">online JSON editor</a>.<br />
				n.b.: if you are verifying a single document, but get parse errors for mutiple documents, your JSON is most likely broken.
			</p>
			<p>
				make sure you select the right validation scheme. the validator cannot verify if you've selected the right schema to match your document, just whether your document matches the schema you've selected (a mismatch will not necessarilly cause a lot of errors on the first check, as it doesn't look at deeper levels if the root element fails verification).
			</p>
			<p>
				<u>be aware</u> of the remarks on date-time formats and duplicate elements in the <a href="https://github.com/naturalis/nba_json_schemas/blob/master/README.md" target="_blank">readme of the repo</a>.
			</p>
			<p>
				<ul>
					<li><a href="https://github.com/naturalis/nba_json_schemas" target="_blank">NBA JSON schema repo @ GitHub</a></li>
					<li><a href="http://api.biodiversitydata.nl/v2/" target="_blank">Naturalis Biodiversity API</a></li>
				</ul>
			</p>
		</div>
	</div>
	<br clear="all" />

	<pre id="errors">
<?php
	echo '<span class="errors validate">validated ',count($docs),' doc(s)';
	if (!empty($schema)) echo ' against schema <span class="errors schema">', $schemas[$schema] ,"</span>";
	echo "</span>\n";

	if(empty($errors)) {
		echo '<span class="errors header">no errors</span>',"\n";
	}
	else {
		echo '<span class="errors header">errors:</span>',"\n";
		foreach((array)$errors as $line=>$errs) {
			echo '<span class="errors document">document #',($line+1),"</span>\n";
			foreach((array)$errs as $errno=>$err) {
				echo '<span class="errors error">error ',($errno+1),"</span>\n";
				foreach((array)$err as $key=>$val) {
					if ($key=='file' || $key=='line') continue;
					$val=is_array($val)?implode("; ",$val):$val;
					echo '<span class="errors key">',$key,'</span>: <span class="errors val">',$val,"</span>\n";
				}
				echo '<span class="errors divider">&nbsp;</span>',"\n";
			}
		}
	}
?>
	</pre>
</body>
</html>
