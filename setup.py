from setuptools import setup, find_packages

setup(
    name="mi-dataset",
    version="0.1.1",
    description="Contains marine integrations dataset drivers and parsers",
    url="http://github.com/oceanobservatories/mi-dataset",
    license="BSD",
    packages=find_packages(),
    package_data={
        '': ['*.yml'],
    },
    install_requires=[
        "pyyaml",
        "numpy",
        "msgpack-python",
        "ntplib",
        "matplotlib",
        "nose"
    ]
)
