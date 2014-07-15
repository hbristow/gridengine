from setuptools import setup

def read(file):
  """Read the contents of a file into a string"""
  with open(file, 'r') as f:
    return f.read()

# ----------------------------------------------------------------------------
# GridEngine Setup
# ----------------------------------------------------------------------------
setup(name='gridengine',
  version='0.1',
  description='High-level python wrapper for the Sun Grid Engine (SGE) using DRMAA and ZMQ',
  long_description=read('README.md'),
  keywords='drmaa sge cluster distributed parallel',
  url='https://github.com/hbristow/gridengine/',
  author='Hilton Bristow',
  author_email='hilton.bristow+gridengine@gmail.com',
  license='GPL',
  packages=[
    'gridengine'
  ],
  package_data={
    'gridengine': ['wrapper.sh']
  },
  install_requires=[
    'drmaa>=0.7.6',
    'pyzmq>=14.1.1'
  ],
  extras_require={
    # advanced serialization support
    'dill': ['dill-0.2b1']
  },
  zip_safe=False
)
