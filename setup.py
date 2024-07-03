from setuptools import setup, find_packages

def read(f):
    with open(f, 'r', encoding='utf-8') as file:
        return file.read()

setup(
    name='minio_spark',
    version='0.8.0',
    description='An abstraction for working with DataLake using MinIO and PySpark.',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Jailton Paiva',
    author_email='jailtoncarlos@gmail.com',
    keywords=["spark", "minio", "kubernet", "datalake"],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries",
    ],
    python_requires='>=3.8',
    install_requires=read('requirements.txt'),
    packages=find_packages(exclude=['tests*']),  # This will automatically include all packages and subpackages
    # download_url="https://github.com/jailtoncarlos/minio_spark/tags",
    url='https://github.com/jailtoncarlos/minio_spark',
    include_package_data=True,  # Includes package data as specified in MANIFEST.in
)
