#!/bin/bash
mkdir fake_gps
for file in "."/gps/*gps*
do
    head -100000 $file > "."/fake_gps/$(basename $file) & 
done

