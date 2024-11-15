# setup.py
from setuptools import setup, find_packages

setup(
    name="my_tool", 
    version="0.1", 
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'my_tool=main:main',
        ],
    },
    install_requires=[
        "databricks-sql-connector",
        "pandas",
        "python-dotenv",
    ],
    author="Allen Wang",
)
