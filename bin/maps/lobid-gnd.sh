#! /bin/bash

set -e

d="${OUTPUT_DIRECTORY:-src/main/resources/transformation/maps}"
p="$d/lobid-gnd"

e=lmdb
f="$p.$e"
t="$p.$$.$e"

curl --no-progress-meter\
  'https://lobid.org/gnd/search?q=*&format=jsonl' |\
  ruby -rjson -e '
    ARGF.each { |r|
      k, *v = JSON.parse(r).values_at(*%w[gndIdentifier preferredName variantName])

      v.compact!
      v.flatten!

      puts "#{k}\u001D#{v.join("\u001F")}" if k && !v.empty?
    }
  ' |\
  ./gradlew execLmdb --args="$t"

[ -s "$t" ] && mv "$t" "$f"

./gradlew execLmdb --args="$f"
