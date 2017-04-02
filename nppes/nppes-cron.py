import datetime
import os.path
import time
import urllib
import logging

import load_nppes
import schedule

nppes_url = 'http://download.cms.gov/nppes'
nppes_file_name_template = 'NPPES_Data_Dissemination_{}_{}.zip'


def generate_nppes_url(file_name):
    return '{}/{}'.format(nppes_url, file_name or generate_nppes_file_name())

def generate_nppes_file_name():
    currdate = datetime.datetime.now() - datetime.timedelta(days=5)
    return nppes_file_name_template.format(currdate.strftime("%B"), currdate.strftime("%Y"))

def generate_nppes_path(file_name):
    return '{}/{}'.format('/code/data', file_name)

def load_providers():
    logging.warning('test')
    nppes_file_name = generate_nppes_file_name()
    nppes_path = generate_nppes_path(nppes_file_name)
    nppes_dl_url = generate_nppes_url(nppes_file_name)

    if not os.path.exists(nppes_path):
        logging.warning('does not exist')
        testfile = urllib.URLopener()
        testfile.retrieve(nppes_dl_url, nppes_path)

    load_nppes.loadFiles(nppes_path, '/code/data/taxonomy.csv')

schedule.every(32).days.do(load_providers)

time.sleep(30)
load_providers()

while True:
    schedule.run_pending()
    time.sleep(1)
