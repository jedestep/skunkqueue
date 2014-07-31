from setuptools import setup

requires = [
    "pymongo",
    "redis",
    "dill"
]

setup(
    name="skunkqueue",
    version="0.3.0",
    packages=["skunkqueue", "skunkqueue.persistence"],
    description="Asynchronous, persistent task runner",
)
