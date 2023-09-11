# Limetrans - Library Metadata Transformation

[![Travis CI](https://travis-ci.org/hbz/limetrans.svg)](https://travis-ci.org/hbz/limetrans)
[![SonarCloud](https://sonarcloud.io/api/project_badges/measure?project=hbz.introx.direct%3Alimetrans&metric=alert_status)](https://sonarcloud.io/dashboard?id=hbz.introx.direct:limetrans)

## Configuration

Limetrans can be regarded as a configuration frame for the use of [Metafacture](https://github.com/metafacture/metafacture-documentation) for library purposes. It makes use of a JSON configuration scheme and can be abstracted as:

```json
{
  "input" : {
    ...
  },
  "transformation-rules" : "...",
  "output": {
    ...
  },
  ...
}
```

### Input

Input is generally configured like this:

```json
"input" : {
  "queue" : {
    "path" : "a/path/to/your/input/file/",
    "pattern" : "your-marc-xml-input-file.xml",
    "sort_by" : "lastmodified",
    "order" : "desc",
    "max" : 1,
    "normalize-unicode" : false,
    "processor" : "MARC21"
  }
}
```

`MARCXML` is the default value for `'processor'` thus `'processor'` can be omitted when processing MARCXML data.

### Transformation

```json
"transformation-rules" : "a/path/to/your/transformation/metafacture/rules/file.xml"
```

### Output

By now, Limetrans is written to be used with [Elasticsearch](https://www.elastic.co/). Therefore, the output object mainly contains an Elasticsearch configuration, besides a JSON output option.

```json
"output": {
  "json" : "a/path/to/your/jsonlines/output/file.jsonl",
  "elasticsearch" : {
    "cluster": "elasticsearch-01",
    "host": ["localhost:9300"],
    "index" : {
      "type" : "title",
      "name" : "choose-your-own-index-name",
      "timewindow" : "yyyyMMdd",
      "settings" : "a/path/to/your/elasticsearch/settings.json",
      "mapping" : "a/path/to/your/elasticsearch/mapping.json",
      "idKey" : "the-id-field-name-configured-in-your-metafacture-rules-file"
    },
    "update" : false,
    "delete" : false,
    "bulkAction" : "index",
    "maxbulkactions" : 100000
  },
  "pretty-printing" : false
}
```

`"type" : "title"` is a suggestion, assuming you might want to transform and store book title information.

### Further configuration

```json
"catalogid" : "choose-your-own-catalog-id",
"collection" : "choose-your-own-collection"
```

Please find examples for the configuration of Limetrans in the [source code](https://github.com/hbz/limetrans/tree/master/src/conf).

## Setup project

### Get Source

```sh
$ git clone git@github.com:hbz/limetrans.git
```

### Setup Elasticsearch

#### [Download and install Elasticsearch](https://www.elastic.co/downloads)

```sh
$ cd third-party
$ wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-2.1.1.zip
$ unzip elasticsearch-2.1.1.zip
$ cd elasticsearch-2.1.1
$ bin/elasticsearch
```

Check with `curl -X GET http://localhost:9200/` if all is well.

#### Configure Elasticsearch

Make sure you have configured the cluster name in `/etc/elasticsearch/elasticsearch.yml` according to your Limetrans configuration.

#### Optionally, you may want to install the [head plugin](https://mobz.github.io/elasticsearch-head/)

```sh
$ cd third-party/elasticsearch-2.4.0
$ bin/plugin install mobz/elasticsearch-head
```

## Contribute

### Coding conventions

Indent blocks by *four spaces* and wrap lines at *100 characters*. For more details, refer to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

### Bug reports

Please file bugs as an issue labeled "Bug" [here](https://github.com/hbz/limetrans/issues/new).
