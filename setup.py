import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="RHEAS",
    version="0.7",
    author="Kostas Andreadis",
    author_email="kandread@umass.edu",
    description="Regional Hydrologic Extremes Assessment System",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kandread/RHEAS",
    packages=setuptools.find_packages(),
    scripts=['scripts/rheas'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
