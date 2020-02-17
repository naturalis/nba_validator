<?php

class inputPrepare {

	private $dirs=[];
	private $processable_dirs=[];
	private $name_changes=[];

	const FILE_UPLOAD_READY = "upload_ready";
	const FILE_PROCESSING = "processing";
	const ARCHIVE_EXTENSIONS = ["gz","zip","tar"];
	const PROCESSABLE_EXTENSIONS = ["json","jsonl","ndjson","txt"];
	const METADATA_FILE_REGEX = "/^metadata[^\.]*\.json$/";

	public function run()
	{
		$this->_getProcessableDirs();
		$this->_unpackArchives();
		$this->_processFiles();		
	}

	public function addDirToPrepare( $dir )
	{
		if (file_exists( $dir ))
		{
			$this->dirs[]=rtrim($dir,"/");
		} 
		else
		{
			throw new Exception(sprintf("dir %s doesn't exist",$file));
		}
	}

	public function getNameChanges()
	{
		return $this->name_changes;
	}

	private function _getProcessableDirs()
	{
		$this->processable_dirs=[];

		foreach ($this->dirs as $dir)
		{
			$this->_printMessage(sprintf("scanning directory %s",$dir));

			$files = glob($dir . "/*");
			$upload_ready=false;
			$process_lock=false;

			foreach($files as $file)
			{
				if (is_dir($file)) continue;
				$upload_ready = $upload_ready || basename($file)==self::FILE_UPLOAD_READY;
				$process_lock = $process_lock || basename($file)==self::FILE_PROCESSING;
			}

			if ($upload_ready)
			{
				$this->processable_dirs[]=$dir;
			}
		}
	}

	private function _unpackArchives()
	{
		foreach ($this->processable_dirs as $dir)
		{
			$this->_printMessage(sprintf("processing directory %s",$dir));

			// GLOB_BRACE is unrecognized in php:7.2-alpine container
			// $files = glob($dir . "/*.{".implode(",",self::ARCHIVE_EXTENSIONS)."}", GLOB_BRACE);
			$files = $this->getFileList($dir,self::ARCHIVE_EXTENSIONS);

			foreach($files as $file)
			{
				$ext=strtolower(pathinfo($file, PATHINFO_EXTENSION));
				$path=pathinfo($file, PATHINFO_DIRNAME);

				if ($ext=="gz")
				{
					// *.tar.gz
					if (strtolower(pathinfo(pathinfo($file, PATHINFO_FILENAME), PATHINFO_EXTENSION))=="tar")
					{
						$this->_printMessage(sprintf("unpacking and deleting %s",$file));
						passthru("tar xvzf " . $file . " -C " . $path);
						unlink($file);
					}
					// *.gz
					else
					{
						$this->_printMessage(sprintf("unpacking %s",$file));
						passthru("gzip -d -f " . $file );
					}
				} 
				else
				if ($ext=="tar")
				{
					$this->_printMessage(sprintf("unpacking and deleting %s",$file));
					passthru("tar -xf " . $file . " -C " . $path);
					unlink($file);
				} 
				else
				if ($ext=="zip")
				{
					$this->_printMessage(sprintf("unpacking and deleting %s",$file));
					passthru("unzip -o " . $file . " -d " . $path);
					unlink($file);
				}
			}
		}
	}

	private function _processFiles()
	{
		foreach ($this->processable_dirs as $dir)
		{
			// GLOB_BRACE is unrecognized in php:7.2-alpine container
			// $files = glob($dir . "/*.{".implode(",",self::PROCESSABLE_EXTENSIONS)."}", GLOB_BRACE);
			$files = $this->getFileList($dir,self::PROCESSABLE_EXTENSIONS);

			foreach($files as $file)
			{
				$ext=strtolower(pathinfo($file, PATHINFO_EXTENSION));

				if (preg_match(self::METADATA_FILE_REGEX,basename($file)))
				{
					// metadata file -> leave unchanged
				}
				else
				if ($ext=="json")
				{
					if ($this->_fileIsJsonl($file))
					{
						// json --> jsonl
						$this->_printMessage(sprintf("changing %s to %s",$file,"jsonl"));
						$this->name_changes[]=$this->_changeFileExtension($file, "jsonl");
					}			
					else
					{
						// assuming single-document JSON-file
						$b=$this->_singleJsonToJsonl($file);
						if ($b!==false)
						{
							$this->name_changes[]=$b;
							$this->_printMessage(sprintf("rewrote %s to %s",$file, "jsonl"));
						}
					}
				} 
				else
				if ($ext=="jsonl" || $ext=="ndjson")
				{
					// assuming valid jsonl / ndjson
				} 
				else
				if ($ext=="txt")
				{
					if ($this->_fileIsJsonl($file))
					{
						// txt --> jsonl
						$this->_printMessage(sprintf("changing %s to %s",$file,"jsonl"));
						$this->name_changes[]=$this->_changeFileExtension($file, "jsonl");
					}			
					else
					{
						// assuming index file of file with deleted unitID's
					}
				}
			}
		}

	}

	private function _fileIsJsonl($file) 
	{
		$handle=fopen($file,"r");
		$line=fgets($handle, 200000);
		fclose($handle);
		return !is_null(json_decode($line));
	}

	private function _changeFileExtension($file, $new_extension)
	{
		$p=pathinfo($file);
		$new = $p["dirname"] . "/" . $p["filename"] . "." . ltrim($new_extension,".");
		$i=1;
		while(file_exists($new))
		{
			$new = $p["dirname"] . "/" . $p["filename"] . "_" . $i++ . "." . ltrim($new_extension,".");
		}

		rename($file,$new);

		return ["old"=>$file,"new" =>$new];
	}

	private function _printMessage( $msg, $write_to_log=false )
	{
		echo $msg, "\n";
	}

	private function _singleJsonToJsonl( $file ) 
	{
		$b = trim(file_get_contents($file));

		if (!is_null(json_decode($b)) && (substr_count($b,"\n")!=0 || substr_count($b,"\r")!=0))
		{
			if (file_put_contents($file,str_replace(["\n","\r"], "", $b)))
			{
				return $this->_changeFileExtension($file, "jsonl");
			}
		}
		else
		{
			return false;
		}
	}

	private function getFileList( $file_path, $extensions )
	{
		$files=[];
	
		if ($handle = opendir($file_path))
		{
			while (false !== ($entry = readdir($handle)))
			{
				if (preg_match('/\.(' . implode('|',$extensions) .')$/', $entry))
				{
					$files[] = rtrim($file_path, "/") . "/" . $entry;
				}
			}
			closedir($handle);
		}

		return $files;
	}

}

