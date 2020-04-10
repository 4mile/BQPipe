from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as readme_file:
    long_description = readme_file.read()

with open('LICENSE') as f:
    package_license = f.read()

setup(
    name='bqpipe',
    version='0.5.0',

    description='Wrapper around BigQuery & Snowflake libraries to simplify writing to/reading from Pandas DataFrames.',
    long_description=long_description,
    long_description_content_type='text/x-rst',

    url='https://github.com/4mile/BQPipe',
    license=license,
    author='Johnathan Brooks',
    author_email='jb@4mile.io',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Database',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='bigquery google cloud etl data engineering dataframe pandas',
    packages=['bqpipe'],

    python_requires='>=3.7, <4',
    install_requires=[
        'numpy',
        'pandas',
        'google-cloud-bigquery',
        'pyarrow'
    ],

    project_urls={
        'Bug Reports': 'https://github.com/4mile/BQPipe/issues',
        'Source': 'https://github.com/4mile/BQPipe',
    },
)
