#! /bin/bash

set -e

d="${OUTPUT_DIRECTORY:-src/main/resources/transformation/maps}"
p="$d/lobid-organisations"

e=lmdb
f="$p.$e"
t="$p.$$.$e"

curl --no-progress-meter\
  'https://lobid.org/organisations/search?q=_exists_:isil&format=tsv:isil,name&size=25000' |\
  tr $'\t' '\035' |\
  ./gradlew execLmdb --args="$t"

[ -s "$t" ] && mv "$t" "$f"

./gradlew execLmdb --args="$f"
