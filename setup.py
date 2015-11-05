from setuptools import setup, find_packages

setup(
    name="mi-dataset",
    version="0.1.0",
    description="Contains marine integrations dataset drivers and parsers",
    url="http://github.com/oceanobservatories/mi-dataset",
    license="BSD",
    packages=find_packages(),
    install_requires=[
        "pyyaml",
        "numpy",
        "msgpack-python",
        "ntplib",
        #"matplotlib",
        "nose"
    ]
)
