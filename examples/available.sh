#!/bin/bash

. load_config.sh

# Records whose title matches 'piano' and which contain at least
# one copy with (status 0 OR status 7) AND (cic_lib 4 OR circ_lib 5)

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
  "query": {
    "bool": {
      "must": {
        "match": {
          "title_maintitle": "piano"
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
        "copy_status": {
          "terms": {"field": "holdings.status"}
        },
        "copy_location": {
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


