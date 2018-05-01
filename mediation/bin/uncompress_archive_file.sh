#!/bin/bash
#
#


set -x

file_name=$1

[ -f "$file_name" ] && [[ $(file "$file_name") = *"Zip archive data"* ]] && unzip -n "$file_name" && rm -rf "$file_name"
[ -f "$file_name" ] && [[ $(file "$file_name") = *"RAR archive data"* ]] && unrar "$file_name"
[ -f "$file_name" ] && [[ $(file "$file_name") = *"gzip compressed data"* ]] && gunzip "$file_name"
[ -f "$file_name" ] && [[ $(file "$file_name") = *"7-zip archive data"* ]] && 7z e "$file_name" && rm -rf "$file_name"
[ -f "$file_name" ] && [[ $(file "$file_name") = *"tar archive"* ]] && tar -xf "$file_name" && rm -rf "$file_name"
[ -f "$file_name" ] && [[ $(file "$file_name") = *"bzip2 compressed data"* ]] && bzip2 -d "$file_name" 
