from setuptools import setup

project_name = 'transfer_convert'
__version__ = '0.1'

long_description = """
tranfer_convert
===============

A collection of scripts for automating conversion to Photon-HDF5,
archival and analysis of smFRET measurements using multiprocessing (each file
is processed in a different CPU).
"""

setup(
    name = project_name,
    version=__version__,
    author = 'Antonino Ingargiola',
    author_email = 'tritemio@gmail.com',
    url = 'https://github.com/Photon-HDF5/phforge',
    download_url = 'https://github.com/Photon-HDF5/phforge',
    install_requires = ['phconvert', 'ipython', 'nbconvert'],
    license = 'MIT',
    description = ("Automated Photon-HDF5 conversion and analysis of smFRET data."),
    long_description = long_description,
    platforms = ('Windows', 'Linux', 'Mac OS X'),
    classifiers=['Intended Audience :: Science/Research',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 3.6',
                 'Topic :: Scientific/Engineering',
                 ],
    py_modules = ['nbrun', 'analyze', 'transfer', 'batch_convert',
                  'batch_analyze'],
    scripts=['analyze.py', 'transfer.py', 'batch_analyze.py', 'batch_convert.py'],
    #zip_safe = False,
)
