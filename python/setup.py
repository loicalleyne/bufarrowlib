"""Forces a platform-specific (non-pure) wheel for the bundled libbufarrow shared library."""
from setuptools import setup
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(distclass=BinaryDistribution)
