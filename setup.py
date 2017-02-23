from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='python-consul-lock',
    version='1.0.0',
    description='Context manager for Consul locking',
    url='https://github.com/davidk01/python-consul-lock',
    author='david karapetyan',
    author_email='dkarapetyan@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Locking',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    keywords='consul locking lock',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['requests'],
)
