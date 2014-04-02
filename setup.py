from setuptools import setup

def read(file):
  """Read the contents of a file into a string"""
  with open(file, 'r') as f:
    return f.read()

setup(name='gridengine',
  version='0.1',
  description='High-level python wapper for the Sun Grid Engine (SGE) using DRMAA and ZMQ',
  long_description=read('README.md'),
  keywords='drmaa sge cluster distributed parallel',
  url='https://github.com/hbristow/gridengine/',
  author='Hilton Bristow',
  author_email='hilton.bristow@gmail.com',
  license='GPL',
  packages=['gridengine'],
  install_requires=read('requirements.txt').splitlines(),
  zip_safe=False)
