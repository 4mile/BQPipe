from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as readme_file:
    long_description = readme_file.read()

# with open('LICENSE') as f:
#     license = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='bqpipe',
    version='0.3.0',

    description='Wrapper around BigQuery library to simplify writing to/reading from BigQuery to Pandas DataFrames.',
    long_description=long_description,
    long_description_content_type='text/x-rst',

    url='https://github.com/jmbrooks/BQPipe',
    # license=license,
    author='Johnathan Brooks',
    author_email='jb@4mile.io',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Data Engineering :: ETL',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='bigquery looker etl data engineering dataframe',

    package_dir={'': 'bqpipe'},
    packages=find_packages(where='bqpipe'),

    python_requires='>=3.5, <4',
    install_requires=[
        'pandas',
        'google-cloud-bigquery'
    ],
    # extras_require={
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },

    # entry_points={  # Optional
    #     'console_scripts': [
    #         'sample=sample:main',
    #     ],
    # },

    project_urls={  # Optional
        'Bug Reports': 'https://github.com/jmbrooks/BQPipe/issues',
        'Source': 'https://github.com/jmbrooks/BQPipe',
    },
)
