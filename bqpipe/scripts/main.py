import argparse


def main(*args, **kwargs):
    print(argparse.ArgumentParser.__doc__)
    parser = argparse.ArgumentParser(description="Read and write from BigQuery to Pandas DataFrames.")
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_read = subparsers.add_parser('read', help='Read actions from BigQuery to DataFrame')
    parser_write = subparsers.add_parser('write', help='Write actions from DataFrame to BigQuery')
    parser_metadata = subparsers.add_parser('metadata', help='Fetch metadata on BigQuery datasets and tables')

    parser_read.add_argument('method', metavar='m', help='Method to take action', choices=['from-table', 'sql'])
    parser_write.add_argument('method', metavar='m', help='Method to take action',
                              choices=['to-table', 'to-table-with-schema'])

    parser_metadata.add_argument('list-datasets')
    parser_metadata.add_argument('list-tables-in-dataset')
    parser_metadata.add_argument('get-tables-schema')

    parser.add_argument(
        '--loglevel', metavar='l', default='info', help='Log level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
    )
    parser.add_argument(
        '--read-from-table', metavar='rft', help='Read '
    )

    input_args = parser.parse_args(args)


if __name__ == '__main__':
    main(name='jb')
