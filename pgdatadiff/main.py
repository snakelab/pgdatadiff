"""
Usage:
  pgdatadiff --firstdb=<firstconnectionstring> --seconddb=<secondconnectionstring> [--only-data|--only-sequences] [--count-only] [--chunk-size=<size>] [--full-data]
  pgdatadiff --version

Options:
  -h --help             Show this screen.
  --version             Show version.
  --firstdb=postgres://postgres:password@localhost/firstdb        The connection string of the first DB
  --seconddb=postgres://postgres:password@localhost/seconddb         The connection string of the second DB
  --only-data           Only compare data, exclude sequences
  --only-sequences      Only compare seqences, exclude data
  --count-only          Do a quick test based on counts alone
  --full-data           Compare full data (default is a top/bottom comparison)
  --chunk-size=10000    The chunk size when comparing data [default: 10000]
"""

import pkg_resources
from fabulous.color import red

from pgdatadiff.pgdatadiff import DBDiff
from docopt import docopt

from multiprocessing.dummy import Pool as ThreadPool


def doDiff(first, second, chunk_size, count_only, full_data, only_sequences=False, threads= 0, thread_number = 0):
    differ = DBDiff(first, second,
                    chunk_size, count_only, full_data,
                    threads, thread_number)

    if not only_sequences:
        if differ.diff_all_table_data(thread_number):
            return 1
    if not arguments['--only-data']:
        if differ.diff_all_sequences(thread_number):
            return 1
    return 0

def main():
    arguments = docopt(
        __doc__, version=pkg_resources.require("pgdatadiff")[0].version)
    first_db_connection_string=arguments['--firstdb']
    second_db_connection_string=arguments['--seconddb']
    chunk_size=arguments['--chunk-size']
    count_only=arguments['--count-only']
    full_data=arguments['--full-data']
    only_sequences=arguments['--only-sequences']

    if not first_db_connection_string.startswith("postgres://") or \
            not second_db_connection_string.startswith("postgres://"):
        print(red("Only Postgres DBs are supported"))
        return 1


    #doDiff(first_db_connection_string, second_db_connection_string, chunk_size, count_only, full_data, only_sequences, thread_number = 1)

    # ThreadCount should be equal in
    threads = 30
    # Make the Pool of workers
    pool = ThreadPool(threads)
    for thread_number in range(threads):
        #print(thread_number)
        pool.apply_async(doDiff, args=(first_db_connection_string, second_db_connection_string, chunk_size, count_only, full_data, only_sequences, threads, thread_number,))
    pool.close()
    pool.join()


    #differ = DBDiff(first_db_connection_string, second_db_connection_string,
    #                chunk_size=arguments['--chunk-size'],
    #                count_only=arguments['--count-only'],
    #                full_data=arguments['--full-data'],
    #                thread_number=thread_number)

    #if not arguments['--only-sequences']:
    #    if differ.diff_all_table_data():
    #        if differ2.diff_all_table_data():
    #            return 1
    #if not arguments['--only-data']:
    #    if differ.diff_all_sequences():
    #        if differ2.diff_all_sequences():
    #            return 1
    return 0
