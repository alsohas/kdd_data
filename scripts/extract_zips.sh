#!/bin/bash
mkdir gps
for file in "."/*.zip
do
    unzip $file -d ./gps/ &
done

