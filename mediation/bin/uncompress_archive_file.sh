#!/bin/bash
#
#


set -x

file_name=$1

[[ $(file "$file_name") = *"Zip archive data"* ]] && unzip -n "$file_name" && rm -rf "$file_name"
[[ $(file "$file_name") = *"RAR archive data"* ]] && unrar "$file_name"
[[ $(file "$file_name") = *"gzip compressed data"* ]] && gunzip "$file_name"