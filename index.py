#!/usr/bin/env python3

# -----------------------------------------------------------------------
# Create elasticsearch indexes from Evergreen config.metabib_field 
# configuration.
#
# TODO 
# * index some display values for sorting, title/author/etc.

import configparser
import logging
import logging.config
import sys
import argparse
import psycopg2
import time
import copy

import lxml.etree as ET
from io import BytesIO
from datetime import date
from elasticsearch import Elasticsearch

db_conn = None

# TODO: it's possible to index a given field via multiple
# language analyzers.
lang_analyzer = 'english'

# cache of XSL transform docs
xsl_docs = {}
xml_namespaces = {}
# cache of config.metabib_field where search_field = true
search_fields = {}

index_name = None

nonsort_field_classes = ['keyword', 'identifier']

index_def = {
  # Some index fields are hard-coded.
  # Remaining index fields are added dynamically below
  'source': {
    'type': 'integer',
    'index': 'not_analyzed',
    'include_in_all': 'false',
  },
  'create_date': {
    'type': 'date',
    'index': 'not_analyzed',
    'include_in_all': 'false',
  },
  'edit_date': {
    'type': 'date',
    'index': 'not_analyzed',
    'include_in_all': 'false',
  },
  'holdings': {
    'type': 'nested',
    'properties': {
      'status': {
        'type': 'integer',
        'index': 'not_analyzed',
        'include_in_all': 'false',
      },
      'circ_lib': {
        'type': 'integer',
        'index': 'not_analyzed',
        'include_in_all': 'false',
      },
      'location': {
        'type': 'integer',
        'index': 'not_analyzed',
        'include_in_all': 'false',
      },
      'circulate': {
        'type': 'boolean',
        'index': 'not_analyzed',
        'include_in_all': 'false',
      },
      'opac_visible': {
        'type': 'boolean',
        'index': 'not_analyzed',
        'include_in_all': 'false',
      }
    }
  }
}

def get_db_conn():
    global db_conn

    if db_conn is None:

        dbcfg = config['evergreen_db']
        db_conn = psycopg2.connect(
            dbname=dbcfg['dbname'], 
            user=dbcfg['user'],
            password=dbcfg['password'], 
            host=dbcfg['host'],
            port=dbcfg['port']
        )

    return db_conn

def insert_to_elasticsearch(output):
    indexresult = es.index(
        index = index_name, 
        doc_type = 'record', 
        id = output['id'], 
        body = output
    )
    logging.debug(repr(indexresult))

def add_xsl_info(format_name):

    if format_name in xsl_docs:
        return

    logging.info('Adding XSLT for %s', format_name)

    xslcur = get_db_conn().cursor()
    xslcur.execute('''
        SELECT namespace_uri, prefix, xslt
        FROM config.xml_transform 
        WHERE name = '%s' ''' % (format_name))

    xsl_info = xslcur.fetchall()[0] # one match

    ns_uri = xsl_info[0]
    prefix = xsl_info[1]
    xslt   = xsl_info[2]

    xml_namespaces[prefix] = ns_uri

    xsl_docs[format_name] = {}

    if format_name == 'marcxml':
        # No transform needed for marcxml
        return

    xsl_file_ish = BytesIO(bytearray(xslt, 'utf-8'))
    xsl_doc = ET.parse(xsl_file_ish)
    xsl_docs[format_name] = {'transform': ET.XSLT(xsl_doc)}

# Extract the search fields and XSL transforms from the EG database
def get_eg_index_fields():
    sfcur = get_db_conn().cursor()

    sfcur.execute('''
        SELECT field_class, name, xpath, facet_xpath, display_xpath, format, 
            weight, search_field, facet_field
        FROM config.metabib_field
        WHERE (search_field OR facet_field) AND xpath IS NOT NULL
    ''')

    for (field_class, name, xpath, facet_xpath, display_xpath,
            format, weight, search_field, facet_field) in sfcur:

        field_name = '%s|%s' % (field_class, name)
        logging.debug('Inspecting field %s', (field_name))

        # we need the "extra" xpath to get at the actual text values
        # we want to index.  (typically, facet and display xpaths are
        # the same, so just use whichever is available).
        if facet_xpath:
            xpath = xpath + facet_xpath
        elif display_xpath:
            xpath = xpath + display_xpath

        search_fields[field_name] = {
            'field_class': field_class,
            'search_field': search_field,
            'facet_field': facet_field,
            'name': name,
            'format': format,
            'xpath': xpath,
            'weight': weight
        }

        add_xsl_info(format)

def add_eg_field_indexes():

    for field_name, search_def in search_fields.items():

        field_class = search_def['field_class']

        # Assume group fields (title, author, etc.) apply all indexing
        # stategies (lang, folding, raw).  Remove 'raw' sub-index
        # below for groups/fields that don't need them.
        field_index = {
            "type": "text",
            "include_in_all": "false",
            "analyzer": lang_analyzer,
            "fields": {
                "folded": {
                    "type": "text",
                    "analyzer": "folding",
                },
                "raw": {
                    "type": "keyword",
                    "include_in_all": "false",
                }
            }
        }

        # Create the field_class-level grouped field.  Some grouped
        # field_class'es are never used for aggregation and sorting, 
        # so remove the "raw" multi-field.
        if field_class not in index_def:
            field_class_idx = copy.deepcopy(field_index)
            if field_class in nonsort_field_classes:
                del field_class_idx['fields']['raw']
            index_def[field_class] = field_class_idx

        # Create the field-specific index

        # If this is neither a facet field nor a field in a sortable
        # class, avoid creating the 'raw' sub-index
        if (field_class in nonsort_field_classes and
            not search_def['facet_field']):
            del field_index['fields']['raw']

        # Values for all field-speicific indexes are copied to 
        # the field_class-level group index

        field_index['copy_to'] = field_class

        index_def[field_name] = field_index

def create_index():
    # TODO: move some of this into the config file
    es.indices.create(
        index = index_name,
        body = {
            'settings': {
                'number_of_shards': 5,
                'number_of_replicas': 1,
                'analysis': {
                    'analyzer': {
                        'folding': {
                            'filter': ['lowercase', 'asciifolding'],
                            'tokenizer': 'standard',
                        },
                    },
                },
            },
        }
    )

    # Index created, now defined the index fields

    # TODO: add some marc fields and subfields for MARC-based searches?

    add_eg_field_indexes()

    es.indices.put_mapping(
        index = index_name,
        doc_type = 'record',
        body = {'record': {'properties': index_def}}
    )


def index_holdings(record_ids):
    holdings_dict = {}
    holdings_count = 0

    cur = get_db_conn().cursor()

    cur.execute('''
SELECT 
    COUNT(*) AS count,
    acn.record, 
    acp.status AS status, 
    acp.circ_lib AS circ_lib, 
    acp.location AS location,
    acp.circulate AS circulate,
    acp.opac_visible AS opac_visible
FROM asset.copy acp
JOIN asset.call_number acn ON acp.call_number = acn.id
WHERE 
    NOT acp.deleted AND
    NOT acn.deleted AND
    acn.record = ANY(%(record_ids)s::BIGINT[])
GROUP BY 2, 3, 4, 5, 6, 7
''', {'record_ids': record_ids})

    for (   count, record, status, circ_lib, 
            location, circulate, opac_visible) in cur:

        holdings_count += 1

        if record not in holdings_dict:
            holdings_dict[record] = []

        holdings_dict[record].append({
            'count': count,
            'status': status, 
            'circ_lib': circ_lib, 
            'location': location,
            'circulate': circulate,
            'opac_visbile': opac_visible
        })

    logging.info('Fetched %s holdings.' % (holdings_count,))
    return holdings_dict

def extract_record_field_values(marc_xml_doc, output):

    xform_docs = {} # per-record transforms (mods, etc.)

    for field_name, search_def in search_fields.items():

        xsl_name = search_def['format']
        xpath_str = search_def['xpath']
        xform_doc = marc_xml_doc

        if xsl_name != 'marcxml': # no transform needed for marcxml

            if xsl_name not in xform_docs:
                # Transform the MARCXML to the new format
                logging.debug('Transforming bib to %s', (xsl_name))
                xform_docs[xsl_name] = xsl_docs[xsl_name]['transform'](marc_xml_doc)

            xform_doc = xform_docs[xsl_name]

        xpath_res = xform_doc.xpath(xpath_str, namespaces=xml_namespaces)

        field_vals = [elm.text for elm in xpath_res if elm.text is not None]

        logging.debug('Extracted %s = %s' % (field_name, repr(field_vals)))

        output[field_name] = field_vals


def full_index_page(state):
    bib_cur = get_db_conn().cursor()
    index_count = 0
    last_edit_date = state['last_edit_date']
    last_id = state['last_id']

    bib_cur.execute('''
SELECT bre.id, bre.marc, bre.create_date, bre.edit_date, bre.source
FROM biblio.record_entry bre
WHERE (
    NOT bre.deleted
    AND bre.active
    AND (
        %(last_edit_date)s IS NULL
        OR (
            bre.edit_date >= %(last_edit_date)s
            AND bre.id > %(last_id)s
        )
        OR bre.edit_date > %(last_edit_date)s
    )
)
ORDER BY bre.edit_date ASC, bre.id ASC
LIMIT 1000
''', {'last_edit_date': last_edit_date, 'last_id': last_id})

    # Clear last_edit_date, last_id
    state['last_edit_date'] = None
    state['last_id'] = None

    results = bib_cur.fetchall()

    # Get just the record IDs
    record_ids = []
    for result in results:
        record_ids.append(result[0])

    logging.info("Fetched %d records" % (len(record_ids)))

    holdings = index_holdings(record_ids)

    for (bre_id, marc, create_date, edit_date, source) in results:
        index_count += 1

        marc_xml_doc = ET.fromstring(marc)

        output = {}
        output['id'] = bre_id
        output['source'] = source
        output['create_date'] = create_date
        output['edit_date'] = edit_date
        extract_record_field_values(marc_xml_doc, output)

        if bre_id in holdings:
            output['holdings'] = holdings[bre_id]
        else:
            output['holdings'] = []

        logging.debug(repr(output))
        insert_to_elasticsearch(output)

        # Update state vars -- the most recent value of these will be
        # written to the state file after the current loop completes
        state['last_edit_date'] = edit_date
        state['last_id'] = bre_id

    return index_count, state

def full_index():

    # Index a "page" of records at a time
    # loop while number of records indexed != 0
    indexed_count = None
    state = {
        'last_edit_date': None,
        'last_id': 0
    }

    while (indexed_count != 0):
        start_time = time.time()
        (indexed_count, state) = full_index_page(state)
        time_taken = time.time() - start_time

        if (time_taken > 0):
            time_recs_sec = indexed_count / time_taken
        else:
            time_recs_sec = 0

        logging.info('indexed %s records in %.0fs (~%.3f rec/s) '
            'ending with date %s id %s'
            % (indexed_count, time_taken, time_recs_sec,
            state['last_edit_date'], state['last_id'])
        )



# -- start execution here ------------------------------------------------

logging.config.fileConfig('index-config.ini')

parser = argparse.ArgumentParser()
parser.add_argument('--recreate-index', action='store_true')
parser.add_argument('--create-index', action='store_true')
parser.add_argument('--drop-index', action='store_true')
parser.add_argument('--full-index', action='store_true')
parser.add_argument('--incremental-index', action='store_true')
cl_args = parser.parse_args()

config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

index_name = config['elasticsearch']['index']

if (es.ping()):
    logging.info("Connection to elasticsearch OK")

# Pretty much always need these, so pre-load them
get_eg_index_fields()

if (cl_args.recreate_index or cl_args.drop_index):
    if es.indices.exists(index_name):
        logging.info('Dropping index %s', (index_name))
        es.indices.delete(index_name)
    elif cl_args.drop_index:
        logging.info(
            "Index %s does not exist -- nothing to drop" % (index_name))

if (cl_args.recreate_index or cl_args.create_index):

    if (es.indices.exists(index_name) and cl_args.create_index):
        logging.error("Index already exists: %s" % (index_name))
        logging.error(
            "Use --drop-index or --recreate-index to first drop the index")
        sys.exit(1)

    logging.info("Creating index %s" % (index_name))
    create_index()

if cl_args.full_index:
    full_index()



