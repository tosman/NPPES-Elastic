import csv
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import time
import os
import argparse
import zipfile
import logging
from functools import reduce


# move this to somewhere that's easy to change
nppes_mapping = {
    'npi': 'NPI',
    'first_name': 'Provider First Name',
    'last_name': 'Provider Last Name (Legal Name)',
    'mailing_address_1': 'Provider First Line Business Mailing Address',
    'mailing_address_2': 'Provider Second Line Business Mailing Address',
    'city': 'Provider Business Mailing Address City Name',
    'state': 'Provider Business Mailing Address State Name',
    'credentials': 'Provider Credential Text',
	'orginization_name': 'Provider Organization Name (Legal Business Name)'
}

def load_taxonomy(nucc):
    nucc_dict = {}
    with open(nucc) as nucc_file:
        nucc_reader = csv.DictReader(nucc_file)
        for row in nucc_reader:
            code = row['Code']
            classification = row['Classification']
            specialization = row['Specialization']
            if code and classification:
                nucc_dict[code] = classification + " " + specialization
    return nucc_dict


def extract_provider(row, nucc_dict):
    # creates the Lucene "document" to define this provider
    # assumes this is a valid provider
    provider_document = {}

    for (key, value) in nppes_mapping.iteritems():
        provider_document[key] = row.get(value, '')
	
	provider_document['specialities'] = [
		nucc_dict.get(row['Healthcare Provider Taxonomy Code_1'], ''),
		nucc_dict.get(row['Healthcare Provider Taxonomy Code_2'], ''),
		nucc_dict.get(row['Healthcare Provider Taxonomy Code_3'], '')
	]

    return provider_document


def convert_to_json(provider_doc):
    # some kind of funky problem with non-ascii strings here
    # trap and reject any records that aren't full ASCII.
    # fix me!
    try:
        j = json.dumps(provider_doc, ensure_ascii=True)
    except Exception, e:
        j = None
    return j

# create a python iterator for ES's bulk load function


def iter_nppes_data(nppes_file, nucc_dict, convert_to_json):
    # extract directly from the zip file
    zip_file_instance = zipfile.ZipFile(nppes_file, "r", allowZip64=True)
    for zip_info in zip_file_instance.infolist():
        # hack - the name can change, so just use the huge CSV. That's
        # the one
        if zip_info.file_size > 4000000000:
            print "found NPI CSV file = ", zip_info.filename
            # rU = universal newline support!
            content = zip_file_instance.open(zip_info, 'rU')
            reader = csv.DictReader(content)
            for row in reader:
				provider_doc = extract_provider(row, nucc_dict)
				json = convert_to_json(provider_doc)
				if json:
					# action instructs the bulk loader how to handle this
					# record
					action = {
						"_index": "nppes",
						"_type": "provider",
						"_id": provider_doc['npi'],
						"_source": json
					}
					yield action


# main code starts here
def loadFiles(nppes_file, nucc_file):
    nucc_dict = load_taxonomy(nucc_file)

    elastic = Elasticsearch([
        'http://elasticsearch:9200'
    ])

    start = time.time()
    logging.warning("start at", start)

    # invoke ES bulk loader using the iterator
    helpers.bulk(elastic, iter_nppes_data(nppes_file, nucc_dict, convert_to_json))

    logging.warning("total time - seconds", time.time() - start)
