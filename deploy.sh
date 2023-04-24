shopt -s nocasematch
# if environment is not specified in .env, default to VAL
if [[ -z "${ENVIRON}" ]]; then 
    ENVIRON=val
    echo environment=${ENVIRON}
else if ! [[ "${ENVIRON}" =~ ^(val)$ ]]; then 
# user must confirm they want to deploy to an environment that is not VAL
    read -p "Are you sure you want to deploy to ${ENVIRON}? (enter y or n) " -n 1 -r
        if [[ ! $REPLY =~ ^[Yy]$ ]]
        then
            exit 1
        else
            echo ""
        fi
    fi
fi
# if deploying to PROD, check that the version name does not have the user's initials from testing
if [[ "${ENVIRON}" =~ ^(prod)$ ]]; then
    if ! [[ "${VERSION}" =~ ^[1-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$ ]]; then 
        echo "ERROR: Prod deployment must not include initials/characters in version (or must be specified)" ;
        exit 1
    fi
fi
# if the environment specified is not VAL or PROD, do not deploy
if ! [[ "${ENVIRON}" =~ ^(val|prod)$ ]]; then 
    echo "ERROR: Environment is not valid (must be val (default) or prod)" ;
else
    # confirm that the version is specified
    if [[ -z "${VERSION}" ]]; then
        echo "ERROR: Please set `VERSION` variable in .env file."
    # if all checks pass, execute deployment
    else
        echo "Deploying (VERSION=${VERSION}) ..."
        cd dqm/batch
        python reverse_lookup.py
        cd ../..
        rm -rf build/*
        rm -rf *.egg-info/*
        python setup.py bdist_wheel
        echo "databricks --profile ${ENVIRON} fs cp ./dist/dqm-${VERSION}-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/dqm-${VERSION}-py3-none-any.whl --overwrite"
        databricks --profile ${ENVIRON} fs cp ./dist/dqm-${VERSION}-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/dqm-${VERSION}-py3-none-any.whl --overwrite
    fi
fi