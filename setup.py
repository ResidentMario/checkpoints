from setuptools import setup
setup(
  name = 'checkpoints',
  packages = ['checkpoints'], # this must be the same as the name above
  py_modules=['checkpoints'],
  version = '0.0.1',
  description = 'Partial result caching for pandas in Python.',
  author = 'Aleksey Bilogur',
  author_email = 'aleksey.bilogur@residentmar.io',
  url = 'https://github.com/ResidentMario/checkpoints',
  download_url = 'https://github.com/ResidentMario/checkpoints/tarball/0.0.1',
  keywords = ['data', 'data analysis', 'exceptions', 'error handling', 'defensive programming,' 'data science',
              'pandas', 'python', 'jupyter'],
  classifiers = [],
)