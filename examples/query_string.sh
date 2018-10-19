#!/bin/bash

. load_config.sh

# Records whose title matches 'piano' and which contain at least
# one copy with (status 0 OR status 7) AND (cic_lib 4 OR circ_lib 5)

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
  "query": {
    "bool": {
      "must": {
        "query_string": {
          "default_field": "_all",
          "query": "piano && author:mozart"
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
  }
}'


