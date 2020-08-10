<?php

// INDEX_FILE_CREATE_ORDER={"BRAHMS":["L","AMD","U","WAG"]}

$files = [
    "AMD_27-01-2020_at_08-09-10.CSV",
    "L_1_27-01-2020_at_11-25-37.CSV",
    "L_2_27-01-2020_at_13-29-14.CSV",
    "L_3_27-01-2020_at_15-34-00.CSV",
    "L_4_28-01-2020_at_09-03-32.CSV",
    "L_5_29-01-2020_at_07-33-39.CSV",
    "L_6_29-01-2020_at_08-19-50.CSV",
    "U_27-01-2020_at_09-17-42.CSV",
    "WAG_24-01-2020_at_09-16-16.CSV",
];



    $order = [ "L", "AMD", "U", "WAG" ];

usort($files, function($a,$b) use ($order)
{
    $s = '/_\d{2}-\d{2}-\d{4}_at/';
    $a_n = array_search(preg_split($s, $a)[0],$order);
    $b_n = array_search(preg_split($s, $b)[0],$order);
    return $a_n == $b_n ? $a > $b : $a_n > $b_n;    
});

print_r($files);

echo json_encode(["BRAHMS" => $order]);