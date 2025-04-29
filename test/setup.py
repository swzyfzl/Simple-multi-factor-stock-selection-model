from setuptools import setup, find_packages

setup(
    name="test",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "akshare",
        "pandas",
        "numpy",
        "scikit-learn",
        "tqdm"
    ],
) 