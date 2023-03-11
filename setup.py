from setuptools import setup, find_packages

setup(
    name='Salesforce-Spark-Connector',
    version='1.0.0',
    url='https://github.com/liam-clifford/Salesforce-Spark-Connector',
    author='Liam Clifford',
    author_email='liamclifford4@gmail.com',
    description='A Python library for connecting to Salesforce API and storing data in Spark Temporary Views',
    packages=find_packages(),
    install_requires=[
        'simple-salesforce==1.10.1',
        'pyspark==3.3.2',
        'requests==2.27.1',
        'numpy==1.22.0',
        'pandas==1.4.0'
    ],
)
