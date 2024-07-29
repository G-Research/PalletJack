#!/usr/bin/env python
import os
import sys
from codecs import open

from setuptools import setup, find_packages
from distutils.extension import Extension
from Cython.Build import cythonize
import pyarrow
import numpy

# https://cython.readthedocs.io/en/latest/src/userguide/source_files_and_compilation.html#distributing-cython-modules
def no_cythonize(extensions, **_ignore):
    for extension in extensions:
        sources = []
        for sfile in extension.sources:
            path, ext = os.path.splitext(sfile)
            if ext in (".pyx", ".py"):
                if extension.language == "c++":
                    ext = ".cpp"
                else:
                    ext = ".c"
                sfile = path + ext
            sources.append(sfile)
        extension.sources[:] = sources
    return extensions

vcpkg_installed = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'vcpkg_installed', os.getenv('VCPKG_TARGET_TRIPLET', ''))
include_dirs = [os.path.join(vcpkg_installed, 'include'), pyarrow.get_include(), numpy.get_include()]
library_dirs = [os.path.join(vcpkg_installed, 'lib')] + pyarrow.get_library_dirs()

print ("VCPKG_ROOT=", vcpkg_installed)
print ("include_dirs=", include_dirs)
print ("library_dirs=", library_dirs)

extra_compile_args = []
extra_link_args = []
debug = False,

if os.getenv('DEBUG', '') == 'ON':
    extra_compile_args = ["-Og", '-DDEBUG']
    extra_link_args = ["-debug:full"]
    debug = True,

# Define your extension
extensions = [
    Extension( "palletjack.palletjack_cython", ["palletjack/palletjack_cython.pyx", "palletjack/palletjack.cc", "palletjack/parquet_types_palletjack.cpp"],
        include_dirs = include_dirs,  
        library_dirs = library_dirs,
        libraries=["arrow", "parquet", "thriftmd" if sys.platform.startswith('win') else "thrift"], 
        language = "c++",
        extra_compile_args = extra_compile_args + (['/std:c++17'] if sys.platform.startswith('win') else ['-std=c++17']),
        extra_link_args = extra_link_args,
    )
]

CYTHONIZE = True # Always cythonize

if CYTHONIZE:
    compiler_directives = {"language_level": 3, "embedsignature": True}
    extensions = cythonize(extensions, compiler_directives=compiler_directives, gdb_debug=debug, emit_linenums=debug)
else:
    extensions = no_cythonize(extensions)

# Make default named pyarrow shared libs available.
pyarrow.create_library_symlinks()

setup(
    packages=["palletjack"],
    package_dir={"": "."},
    zip_safe=False,
    ext_modules=extensions,
    project_urls={
        "Documentation": "https://github.com/G-Research/PalletJack",
        "Source": "https://github.com/G-Research/PalletJack",
    },
)