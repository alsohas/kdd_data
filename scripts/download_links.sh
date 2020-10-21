#!/bin/bash

counter=0
file_name=""
while read url; do
    echo $((counter++))
    file_name="gps_${counter}.zip"
    curl -o $file_name $url & 
done < links.txt
