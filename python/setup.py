#!/usr/bin/env python
import os
import sys
from codecs import open

from setuptools import setup, find_packages
from distutils.extension import Extension
from setuptools.command.test import test as TestCommand
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

class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass into py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        try:
            from multiprocessing import cpu_count

            self.pytest_args = ["-n", str(cpu_count()), "--boxed"]
        except (ImportError, NotImplementedError):
            self.pytest_args = ["-n", "1", "--boxed"]

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

vcpkg_installed = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'vcpkg_installed', os.getenv('VCPKG_TARGET_TRIPLET', ''))
include_dirs = [os.path.join(vcpkg_installed, 'include'), pyarrow.get_include(), numpy.get_include()]
library_dirs = [os.path.join(vcpkg_installed, 'lib')] + pyarrow.get_library_dirs()

print ("VCPKG_ROOT=", vcpkg_installed)
print ("include_dirs=", include_dirs)
print ("library_dirs=", library_dirs)

extra_compile_args = [] # ['-DDEBUG']

# Define your extension
extensions = [
    Extension( "palletjack.palletjack_cython", ["palletjack/palletjack_cython.pyx", "palletjack/palletjack.cc", "palletjack/parquet_types_palletjack.cpp"],
        include_dirs = include_dirs,  
        library_dirs = library_dirs,
        libraries=["arrow", "parquet", "thriftmd" if sys.platform.startswith('win') else "thrift"], 
        language = "c++",
        extra_compile_args = extra_compile_args + ['/std:c++17'] if sys.platform.startswith('win') else ['-std=c++17'],
    )
]

CYTHONIZE = True # Always cythonize

if CYTHONIZE:
    compiler_directives = {"language_level": 3, "embedsignature": True}
    extensions = cythonize(extensions, compiler_directives=compiler_directives)
else:
    extensions = no_cythonize(extensions)

# Make default named pyarrow shared libs available.
pyarrow.create_library_symlinks()

setup(
    packages=["palletjack"],
    package_dir={"": "."},
    zip_safe=False,
    ext_modules=extensions,
    test_suite = 'test',
    project_urls={
        "Documentation": "https://github.com/marcin-krystianc/PalletJack",
        "Source": "https://github.com/marcin-krystianc/PalletJack",
    },
)