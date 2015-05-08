from setuptools import setup, find_packages

setup(
    name="mi-dataset",
    version="0.0.2",
    description="Contains marine integrations dataset drivers and parsers",
    url="http://github.com/oceanobservatories/mi-dataset",
    license="BSD",
    packages=find_packages(),
    install_requires=[
        "pyyaml",
        "numpy",
        "graypy",
        "msgpack-python",
        "ntplib",
        "matplotlib",
        "nose"
    ]
)
