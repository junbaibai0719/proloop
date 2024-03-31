
from setuptools import Extension, setup, find_packages
from Cython.Build import cythonize


setup(
    name="proloop",
    version="1.0.0",
    ext_modules=cythonize(["proloop/loop.pyx"], annotate=True, language_level='3'),
    zip_safe=False,
    # data_files=[
    #     ("", ["src/__init__.py"])
    # ],
)
