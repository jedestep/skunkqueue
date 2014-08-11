from setuptools import setup

requires = [
    "pymongo",
    "redis",
    "fdb",
    "flask",
    "dill"
]

packages = [
    'skunkqueue',
    'skunkqueue.persistence',
    'skunkqueue.util',
    'skunkqueue.cli',
    'skunkqueue.web',
    'skunkqueue.scheduler',
]

setup(
    name="skunkqueue",
    version="0.5.2",
    packages=packages,
    include_package_data=True,
    install_requires=requires,
    scripts=["scripts/skunq"],
    description="Asynchronous, persistent task runner"
)
