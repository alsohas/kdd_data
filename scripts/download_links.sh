#!/bin/bash

while read url; do
    curl $url -O
done < links.txt
