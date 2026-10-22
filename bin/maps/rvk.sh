#! /bin/bash

set -e

d="${OUTPUT_DIRECTORY:-$PWD/main/src/main/resources/transformation/maps}"
p="$d/rvk"

e=lmdb
f="$p.$e"
t="$p.$$.$e"

# https://rvk.uni-regensburg.de/regensburger-verbundklassifikation-online/rvk-download
curl --no-progress-meter\
  'https://rvk.uni-regensburg.de/downloads/rvko_marcxml_2026_2.xml.gz' |\
  gunzip -c |\
  ${0/.sh/.rb} |\
  ./gradlew execLmdb --args="$t"

[ -s "$t" ] && mv --backup --suffix=.bak "$t" "$f"

./gradlew execLmdb --args="$f"
