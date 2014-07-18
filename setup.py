from setuptools import setup

requires = [
    "pymongo",
    "dill"
]

setup(
    name="skunkqueue",
    version="0.3.0",
    packages=["skunkqueue"],
    description="Asynchronous, persistent task runner",
)
