import os
from setuptools import setup, find_packages
from askmanta import VERSION


f = open(os.path.join(os.path.dirname(__file__), 'README.md'))
readme = f.read()
f.close()

setup(
    name='Ask Manta',
    version=".".join(map(str, VERSION)),
    description="Job runner for Joyent's Manta cloud storage and map-reduce service.",
    long_description=readme,
    author='Stijn Debrouwere',
    author_email='stijn@stdout.be',
    url='http://github.com/stdbrouw/askmanta/tree/master',
    packages=find_packages(),
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ], 
    entry_points = {
          'console_scripts': [
                'askmanta = askmanta.commands:parse', 
          ],
    },
)

