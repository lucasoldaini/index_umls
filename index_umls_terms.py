from __future__ import unicode_literals, division, print_function

import codecs

from utils.config_loader import parse_config
from utils import elastic
from utils.common import timer

# MRCONSO_PATH = '/lustre/irlab/datasets/umls-2015ab/installation/META/MRCONSO.RRF'
MRSTY_PATH = '/lustre/irlab/datasets/umls-2015ab/installation/META/MRSTY.RRF'

MRCONSO_PATH = ('/lustre/irlab/datasets/umls-2015ab/'
                'installation/META/MRCONSO.RRF')

# a description of headers is available at http://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/?report=objectonly
HEADERS_MRCONSO = [
    'cui', 'lat', 'ts', 'lui', 'stt', 'sui', 'ispref', 'aui', 'saui',
    'scui', 'sdui', 'sab', 'tty', 'code', 'str', 'srl', 'suppress', 'cvf']
HEADERS_MRSTY = ['cui', 'sty', 'hier' 'desc', 'sid', 'num']

MAPPING = 'settings/umls_terms.json'
INDEX_NAME = 'umls_terms'
DOC_TYPE = 'concept_term'
# WILL_MAPPING = '/lustre/irlab/will/medline_mappings1.txt'
WILL_MAPPING = '/home/ls988/tmp/MD.txt'

HEADERS_WILL = [
    'concept1', 'CID1', 'semantictype1', 'relation', 'concept2', 'CID2',
    'semantictype2', 'disease'
]

ONLY_ACCEPTED = False

if ONLY_ACCEPTED:
    INDEX_NAME += '_medlineplus'


@timer
def load_semtypes(path=MRSTY_PATH, headers=HEADERS_MRSTY):
    sem_map = {}

    with codecs.open(path, 'rb', 'utf-8') as f:
        for i, ln in enumerate(f):
            content = dict(zip(headers, ln.strip().split('|')))
            sem_map.setdefault(content['cui'], []).append(content['sty'])

    return sem_map


@timer
def will_cuis(path=WILL_MAPPING, headers=HEADERS_WILL):
    accepted_cuis = set()

    with codecs.open(path, 'rb', 'utf-8') as f:
        for i, ln in enumerate(f):
            content = dict(zip(headers, ln.strip().split('|')))
            accepted_cuis.add(content['CID1'])
            accepted_cuis.add(content['CID2'])

    return accepted_cuis


def mrconso_iterator(sem_map, path=MRCONSO_PATH, headers=HEADERS_MRCONSO):
    with codecs.open(path, 'rb', 'utf-8') as f:
        for i, ln in enumerate(f):
            content = dict(zip(headers, ln.strip().split('|')))

            # if content['cui'] not in accepted_cuis and ONLY_ACCEPTED:
                # continue

            if content['lat'] != 'ENG':
                continue

            term = {
                '_id': i,
                '_type': DOC_TYPE,
                'cui': content['cui'],
                'is_preferred': content['ispref'] == 'Y',
                # 'is_medlineplus': content['cui'] in accepted_cuis,
                'text': content['str'],
                'semtype': sem_map.get(content['cui'], [])
            }

            yield term

@timer
def main(opts):
    opts.index_name = INDEX_NAME
    opts.doc_type = DOC_TYPE

    # accepted_cuis = will_cuis()

    es = elastic.get_client_from_opts(opts)

    elastic.create_index(
        index_name=INDEX_NAME, index_settings_path=MAPPING, es_client=es)

    sem_map = load_semtypes()

    mrconso = mrconso_iterator(sem_map)
    elastic.index_in_bulk(documents=mrconso, opts=opts, es_client=es)


if __name__ == '__main__':
    main_opts, _ = parse_config('settings/project.json')
    main(main_opts)
