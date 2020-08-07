<?php

class LogClass {

	private $logFile;
	private $process;
	private $level;
	private $format = '%timestamp% - %process% - %level% - %message%';
	private $dateFormat = 'Y-m-d H:i:s.v';
	private $timestamp;
	private $line;
	private $writeToLog = true;
	private $writeToStdOut = true;
	private $fp;
	private $logLevels = [ "DEBUG" => 0, "INFO" => 1, "WARNING" => 2, "ERROR" => 3 ];
	private $stdOutLevel = "DEBUG";
	private $logLevel = "INFO";

	public function __construct($logFile,$process)
	{
		$this->process = $process;
		$this->logFile = $logFile;
		$this->openFile();
	}

	public function __destruct()
	{
		if ($this->fp)
		{
			fclose($this->fp);
		}
	}

	public function setWriteToLog($state)
	{
		if (is_bool($state))
		{
			$this->writeToLog = $state;
		}
	}

	public function setWriteToStdOut($state)
	{
		if (is_bool($state))
		{
			$this->writeToStdOut = $state;
		}
	}

	public function setLogLevel($level)
	{
		if (array_key_exists($state, $this->logLevels))
		{
			$this->logLevel = $level;
		}
		else
		{
			throw new Exception(sprintf("unknown log level: %s", $state), 1);
		}
	}

	public function setStdOutLevel($level)
	{
		if (array_key_exists($state, $this->logLevels))
		{
			$this->stdOutLevel = $state;
		}
		else
		{
			throw new Exception(sprintf("unknown log level: %s", $state), 1);
		}
	}

	public function info($msg)
	{
		$this->level = "INFO";
		$this->msg = $msg;
		$this->logLine();
	}

	public function error($msg)
	{
		$this->level = "ERROR";
		$this->msg = $msg;
		$this->logLine();
	}

	public function warning($msg)
	{
		$this->level = "WARNING";
		$this->msg = $msg;
		$this->logLine();
	}

	public function debug($msg)
	{
		$this->level = "DEBUG";
		$this->msg = $msg;
		$this->logLine();
	}

	private function logLine()
	{
		$this->setTimestamp();
		$this->makeLine();

		if ($this->writeToLog && ($this->logLevels[$this->level] >= $this->logLevels[$this->logLevel]))
		{
			$this->writeToLog();
		}

		if ($this->writeToStdOut && ($this->logLevels[$this->level] >= $this->logLevels[$this->stdOutLevel]))
		{
			$this->writeToStdOut();
		}
	}

	private function makeLine()
	{
		$this->line = str_replace(
			[
				'%timestamp%',
				'%process%',
				'%level%',
				'%message%'
			],
			[
				$this->timestamp->format($this->dateFormat),
				$this->process,
				$this->level,
				$this->msg
			],
			$this->format
		);
	}

	private function setTimestamp()
	{
		$this->timestamp = new DateTime();
	}

	private function writeToLog()
	{	
		fwrite($this->fp, $this->line . "\n");
	}

	private function openFile()
	{
		$this->fp = fopen($this->logFile, 'a');
	}

	private function writeToStdOut()
	{
		echo $this->line,"\n";
	}

}
