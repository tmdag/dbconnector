from setuptools import setup

setup(name='dbconnector',
      version='0.15',
      description='helper modules for MySQL DB connector',
      url='http://github.com/tmdag/dbconnector',
      author='Albert Szostkiewicz',
      author_email='tmdag@tmdag.com',
      license='MIT',
      packages=['dbconnector'],
      install_requires=['mysql-connector-python'],
      zip_safe=False)