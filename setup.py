import os

from setuptools import find_packages
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as fp:
    long_description = fp.read()


setup(
    name='stitches-gis',
    author='Dave Poulter',
    author_email='hello@davepoulter.net',
    url='https://github.com/davebrent/stitches',
    version='0.0.1',
    license='GPLv3',
    description='A tool for developing GIS processing pipelines with GRASS',
    long_description=long_description,
    packages=find_packages(exclude=['tests*']),
    entry_points={
        'console_scripts': [
            'stitches=stitches:main'
        ]
    },
    install_requires=[
        'colorful',
        'docopt',
        'grass-session',
        'jinja2',
        'toml',
        'wurlitzer',
    ],
    tests_require=[
        'pylint',
        'pytest',
    ],
    keywords='gis grass-gis task-runner',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Topic :: Scientific/Engineering :: GIS',
    ],
)
