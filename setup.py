from setuptools import setup

requires = [
    "pymongo",
    "redis",
    "fdb",
    "dill"
]

setup(
    name="skunkqueue",
    version="0.5.2",
    packages=["skunkqueue", "skunkqueue.persistence", "skunkqueue.util", "skunkqueue.cli"],
    scripts=["scripts/skunq"],
    description="Asynchronous, persistent task runner",
)
