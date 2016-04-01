#!/bin/bash

. load_config.sh

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
    "query": {
        "filtered": {
            "query": {
                "match": {"_all": "smith"}
            },
            "filter": {
              "nested": {
                "path": "holdings",
                "filter": {
                  "term": {"holdings.status": "Available"}
                }
              }
            }
        }
    },
    "aggs": {
        "genres": {
            "terms": {
                "field": "genres"
            }
        },
        "authors": {
            "terms": {
                "field": "author"
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
