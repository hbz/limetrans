#! /bin/bash

d="${OUTPUT_DIRECTORY:-src/main/resources/transformation/maps}"
f="$d/lobid-gnd.lmdb"

rm -f "$f"

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
  ./gradlew execLmdb --args="$f"
