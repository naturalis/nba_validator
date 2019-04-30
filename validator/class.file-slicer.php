<?php

	class fileSlicer
	{
		private $_parallel_processes = 4;
		private $_files = [];
		private $_total_lines = 0;
		private $_slice_size = 0;
		private $_slices = [];
		private $_do_process = true;
		
		public function setNumberOfParallelProcesses( $parallel_processes )
		{
			if (is_int($parallel_processes))
			{
				$this->_parallel_processes = $parallel_processes;
			}
		}

		public function getSlices()
		{
			return $this->_slices;
		}

		public function getOverallNumbers()
		{
			return [
				"total_lines" => $this->_total_lines,
				"slice_size" => $this->_slice_size,
				"slices" => count($this->_slices),
			];
		}

		public function reset()
		{
			$this->_files=[];
			$this->_total_lines = 0;
			$this->_slice_size = 0;
			$this->_slices=[];
		}

		public function addFile( $path, $original_path, $lines=0 )
		{
			if ($lines<=0)
			{
				$lines = intval(exec("cat " . $path . " | wc -l"));
			}

			$this->_files[]=[ "path" => $path, "lines" => $lines, "original_path" => $original_path ];
		}

		public function calculateSplit()
		{
			$this->_getTotalLines();
			$this->_doCalculateSplit();
		}

		private function _getTotalLines()
		{
			foreach($this->_files as $file)
			{
				$this->_total_lines += $file["lines"];
			}
		}

		private function _doCalculateSplit()
		{
			$this->_slice_size = ceil($this->_total_lines / $this->_parallel_processes);
			$rest = 0;
			$slice_index = 0;
			$tmp_total_lines = $this->_total_lines;
			$index=0;

			while($tmp_total_lines>0)
			{
				foreach($this->_files as $file)
				{
					if ($rest>0)
					{
						$rest_slice = $this->_slice_size - $rest;
						$this->_slices[$slice_index++]["morsels"][]=[
							"path" => $file["path"],
							"original_path" => $file["original_path"],
							"start" => 0,
							"size" => min([$rest_slice,$file["lines"]]),
							"index" => $index++
						];
						$tmp_total_lines -= min([$rest_slice,$file["lines"]]);
					}
					else
					{
						$rest_slice = 0;
					}

					$overshoot = $rest_slice - $file["lines"];
					if ($overshoot > 0)
					{
						$rest = $overshoot;
						continue;
					}

					$rest = ($file["lines"] - $rest_slice) % $this->_slice_size;
					$cycles = ($file["lines"] - $rest_slice - $rest) / $this->_slice_size;

					for($n=0;$n<$cycles;$n++)
					{
						$this->_slices[$slice_index++]["morsels"][]=[
							"path" => $file["path"],
							"original_path" => $file["original_path"],
							"start" => ($n * $this->_slice_size) + $rest_slice,
							"size" => $this->_slice_size,
							"index" => $index++
						];
						$tmp_total_lines -= $file["lines"];
					}

					if ($rest>0)
					{
						$this->_slices[$slice_index]["morsels"][]=[
							"path" => $file["path"],
							"original_path" => $file["original_path"],
							"start" => ($n * $this->_slice_size) + $rest_slice,
							"size" => $rest,
							"index" => $index++
						];						
						$tmp_total_lines -= $rest;
					}
				}
			}

			foreach ($this->_slices as $key=>$slice)
			{
				foreach ($slice["morsels"] as $morsel)
				{
					if (isset($this->_slices[$key]["size"]))
					{
						$this->_slices[$key]["size"] += $morsel["size"];
					}
					else
					{
						$this->_slices[$key]["index"] = $key;
						$this->_slices[$key]["status"] = "pending";
						$this->_slices[$key]["size"] = $morsel["size"];
					}
				}
			}
		}
	}