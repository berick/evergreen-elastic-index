#!/bin/bash

. load_config.sh

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
  "query": {
    "bool": {
      "must": {
        "match": {
          "title": "piano"
        }
      },
      "filter": {
        "nested": {
          "path": "holdings",
          "query": {
            "terms": {"holdings.status": ["0", "7"]}
          }
        }
      }
    }
  },
  "aggs": {
    "genres": {
      "terms": {
        "field": "genres.raw"
      }
    },
    "series": {
      "terms": {
        "field": "series.raw"
      }
    },
    "authors": {
      "terms": {
        "field": "author.raw"
      }
    },
    "holdings": {
      "nested": {
        "path": "holdings"
      },
      "aggs": {
        "availability": {
          "terms": {"field": "holdings.status"}
        },
        "location": {
          "terms": {"field": "holdings.location"}
        }

      }
    },
    "type_of_resource": {
      "terms": {
        "field": "type_of_resource"
      }
    }
  }
}'


