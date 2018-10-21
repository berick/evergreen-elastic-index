#!/bin/bash

. load_config.sh

# limit returned fields to just record ID
# "_source": ["id"]


curl -s -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
  "sort": [
    {"title.raw": "asc"},
    {"author.raw": "asc"},
    "_score"
  ],
  "query": {
    "bool": {
      "must": {
        "query_string": {
          "default_field": "keyword",
          "query": "(piano && author:mozart) || (ready && author:cline)"
        }
      },
      "filter": {
        "nested": {
          "path": "holdings",
          "query": {
            "bool": {
              "must": [
                {
                  "bool": {
                    "should": [
                      {"term": {"holdings.status": "0"}},
                      {"term": {"holdings.status": "7"}}
                    ]
                  }
                },
                {
                  "bool": {
                    "should": [
                      {"term": {"holdings.circ_lib": "4"}},
                      {"term": {"holdings.circ_lib": "5"}}
                    ]
                  }
                }
              ]
            }
          }
        }
      }
    }
  },
  "aggs": {
    "genres": {
      "terms": {
        "field": "identifier|genre.raw"
      }
    },
    "subject|topic": {
      "terms": {
        "field": "subject|topic.raw"
      }
    }
  }
}'


