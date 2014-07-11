from setuptools import setup

requires = [
    "pymongo",
    "dill"
]

setup(
    name="skunkqueue",
    version="0.0.1",
    packages=["skunkqueue"],
    description="Asynchronous task runner with MongoDB backend",
)