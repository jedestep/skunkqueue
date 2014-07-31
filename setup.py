from setuptools import setup

requires = [
    "pymongo",
    "redis",
    "dill"
]

setup(
    name="skunkqueue",
    version="0.5.0",
    packages=["skunkqueue", "skunkqueue.persistence", "skunkqueue.util"],
    description="Asynchronous, persistent task runner",
)
