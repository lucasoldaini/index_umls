from __future__ import unicode_literals, division, print_function

import codecs

from utils import elastic

MRCONSO_PATH = '/mnt/blue-hd/ls/datasets/UMLS/full/META/MRCONSO.RRF'

# a description of headers is available at
# http://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/?report=objectonly
HEADERS = ['cui', 'lat', 'ts', 'lui', 'stt', 'sui', 'ispref', 'aui', 'saui',
           'scui', 'sdui', 'sab', 'tty', 'code', 'str', 'srl', 'suppress',
           'cvf']

MAPPING = 'umls.settings.json'
INDEX_NAME = 'umls'
DOC_TYPE = 'concept'


def mrconso_iterator(path=MRCONSO_PATH, headers=HEADERS):
    current_concept = {}
    with codecs.open(path, 'rb', 'utf-8') as f:
        for ln in f:
            content = dict(zip(headers, ln.strip().split('|')))

            yield_current_concept = (
                len(current_concept) > 0 and
                current_concept['_id'] != content['cui'])

            if yield_current_concept:
                yield current_concept
                current_concept.clear()

            if content['lat'] != 'ENG':
                continue

            current_concept.setdefault('_id', content['cui'])
            current_concept['_type'] = 'concept'

            if content['ispref'] == 'Y':
                current_concept['preferred'] = content['str']

            current_concept.setdefault('atoms', []).append(content['str'])

    yield current_concept


def main():
    es = elastic.get_client(
        index_name=INDEX_NAME,
        doc_type=DOC_TYPE,
        timeout=120
    )

    elastic.create_index(
        index_name=INDEX_NAME,
        index_settings=MAPPING,
        es_client=es
    )

    mrconso = mrconso_iterator()
    elastic.index_in_bulk(documents=mrconso, verbose=True, es_client=es)


if __name__ == '__main__':
    main()
