from setuptools import setup, find_packages

setup(
    name='minio_datalake',
    version='0.1.0',
    description='An abstraction for working with DataLake using MinIO and PySpark.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Jailton Paiva',
    author_email='jailtoncarlos@gmail.com',
    url='https://github.com/jailtoncarlos/minio_datalake',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'minio',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
