#!/bin/bash

newfile='all_matches_2024.json'
dir="/Users/riker/Python/VLRValorant/jason_files_matches/"

echo "[" >> $newfile

$count = 0
for file in `ls -1 $dir`
do
    echo $file
    if [ $count > 1 ] ; then
        echo "," >> $newfile
    fi
    echo "\n" >> $newfile
    cat "$dir$file" >> $newfile
    count=$((count + 1))
done

echo "]" >> $newfile
