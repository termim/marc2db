from distutils.core import setup

setup(name='marcout',
      version='1.0',
      description='Extract data from MARC-21 data',
      packages=['pymarc'],
      scripts=['marc2db.py']
     )
