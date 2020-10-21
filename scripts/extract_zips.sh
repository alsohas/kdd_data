#!/bin/bash
for file in "."/*.zip
do
    unzip $file -d ./gps/ &
done

