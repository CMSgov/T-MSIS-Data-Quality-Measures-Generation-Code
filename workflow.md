# Workflow

This documents how to build and deploy the wheel file.

## Automated Workflow

Automation is still being built and this document will need to be updated as we go.

### Current

When a Pull Request is merged to the master branch, GitHub is configured to trigger a [Jenkins job](https://jenkins.macbisdw.cmscloud.local/job/build-and-deploy-dq-measures-python/) that automatically builds the Jenkinsfile that is in the root directory of this repository.

This job will:

1. Launch a Docker container using the Dockerfile to run the Jenkins job in.
1. Build the wheel and version it according to the version number parameter configured in setup.py.
1. Upload the wheel file to s3 bucket attached to databricks clusters:
    - s3://aws-hhs-cms-cmcs-databricks-dev/databricks-macbis-dev/0/FileStore/shared_uploads/dqm/
    - s3://aws-hhs-cms-cmcs-databricks-dev/databricks-macbis-val/0/FileStore/shared_uploads/dqm/
    - s3://aws-hhs-cms-cmcs-databricks-dev/databricks-macbis-prod/0/FileStore/shared_uploads/dqm/

### To Be Done

These are ideas that haven't been executed yet

Deploying wheels to clusters should be triggered manually. We can't assume anything about what particular version of the library people want on particular clusters at a particular time.

#### Deploy to All-purpose Clusters

Deploying a wheel to all-purpose clusters can be done by running a different Jenkins job than the automatically triggered one. It will edit the json files by using a terraform variable to update the location and version of the wheel library. Those json files go into a notebook? Then the Jenkins job will run the notebook?

- This job should give the user an option to choose the version of the library via a text box. If the version they choose doesn't exist in s3 then the job will just fail.
- Another option to choose the environment (val/state-prod/prod).

We would need to have a way to give Mathmatic

#### Deploy to Interactive Clusters

Deploying wheels to interactive clusters can be done via notebooks. Maybe using the %pip command?

#### Cleanup

We could create an automated cleanup job that runs nightly. It could keep the last 5 versions of the wheel?

We could clean up the s3 folders.
We could cleanup the clusters too because the installed library wheels will accumulate.


## Local Workflow

### Build the library

DQ Measures is deployed as distributable WHL ("Wheel") file. WHL files are built using [_setuptools_](https://pypi.org/project/setuptools/)

If not done so already, run these commands:

1. > ```python -m venv .venv```
2. > ```.venv/Scripts/Activate.ps1```
3. > ```python -m pip install --upgrade pip```
4. > ```python -m pip install -r requirements.txt```

From the top level folder, run these commands:

5. > ```rm -r -fo .\build; rm -r -fo .\*.egg-info``` (only if you have created a wheel file before)
6. > ```python setup.py bdist_wheel```

#### Upload the WHL file to the Databricks environment

7. > ```databricks --profile val fs cp ./dist/dqm-2.6.10-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/dqm-2.6.10-py3-none-any.whl --overwrite```

### Deploy the library to the Databricks cluster

Deploy the library WHL file to Databricks clusters using [these](https://docs.databricks.com/libraries/cluster-libraries.html) instructions where applicable.

## Testing

(there is a testing automation effort that needs to be described here)
