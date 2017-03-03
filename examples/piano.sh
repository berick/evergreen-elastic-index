#!/bin/bash

. load_config.sh

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
    "query": {
        "match": {"title": "piano"}
    },
    "aggs": {
        "series": {
            "terms": {
                "field": "series.raw"
            }
        },
        "genres": {
            "terms": {
                "field": "genres.raw"
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
