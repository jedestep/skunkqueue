from setuptools import setup

requires = [
    "pymongo",
    "redis",
    "fdb",
    "dill"
]

packages = [
    'skunkqueue',
    'skunkqueue.persistence',
    'skunkqueue.util',
    'skunkqueue.cli',
    'skunkqueue.web'
]

setup(
    name="skunkqueue",
    version="0.5.2",
    packages=packages,
    include_package_data=True,
    scripts=["scripts/skunq"],
    description="Asynchronous, persistent task runner"
)
