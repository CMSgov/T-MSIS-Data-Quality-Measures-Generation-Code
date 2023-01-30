import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dqm",
    version="3.5.00",
    author="Sarah Schoenfeldt",
    author_email="Sarah.Schoenfeldt@cms.hhs.gov",
    description="A package to calculate data quality measures on T-MSIS data using Databricks",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    url="https://git-codecommit.us-east-2.amazonaws.com/v1/repos/DQ-Measures",
    classifiers=[
        "Programming Language :: Python :: 3.9.13 :: Only",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    package_data={
        "dqm": ["cfg/*.pkl", "batch/*.pkl", "testing/*.pkl"],
    },
    project_urls={
        'Documentation': 'https://tmsis2.atlassian.net/wiki/spaces/DQM/pages/2758180868/Data+Quality+Measures+Python+Library',
        'Tracker': 'https://tmsis2.atlassian.net/browse/DQM',
    },
    python_requires=">=3.8",
)
