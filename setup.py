from setuptools import setup, find_packages

setup(
    name="azsql_package", 
    version="0.1.0",
    author="Chad Thweatt",
    author_email="ChadT1872@gmail.com",
    description="A package for handling SQL connections with Azure SQL",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ChadT1872/azsql.git", 
    packages=find_packages(),
    install_requires=[
        "msal",
        "pyodbc",
        "pandas",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
