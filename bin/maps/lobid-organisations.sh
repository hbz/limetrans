#! /bin/bash

f=src/main/resources/transformation/maps/lobid-organisations.lmdb

rm -f "$f"

curl --no-progress-meter\
  'https://lobid.org/organisations/search?q=_exists_:isil&format=tsv:isil,name&size=25000' |\
  tr $'\t' '\035' |\
  ./gradlew execLmdb --args="$f"
