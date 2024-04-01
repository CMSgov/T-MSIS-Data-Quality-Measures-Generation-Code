shopt -s nocasematch
# if environment is not specified in .env, default to VAL
if [[ -z "${ENVIRON}" ]]; then 
    ENVIRON=e2val
    echo environment=${ENVIRON}
else if ! [[ "${ENVIRON}" =~ ^(e2val)$ ]]; then 
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
# if deploying to PROD, check that the version name does not have the user's initials from testing and is on the master branch
if [[ "${ENVIRON}" =~ ^(e2prod)$ ]]; then
    if ! [[ "${VERSION}" =~ ^[1-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$ ]]; then 
        echo "ERROR: Prod deployment must not include initials/characters in version (or must be specified)" ;
        exit 1
    fi
    if [[ "$(git rev-parse --abbrev-ref HEAD)" != "master" ]]; then
        echo "ERROR: Prod deployment must be on master branch";
        exit 1;
    fi
fi
# if the environment specified is not VAL or PROD, do not deploy
if ! [[ "${ENVIRON}" =~ ^(e2val|e2prod)$ ]]; then 
    echo "ERROR: Environment is not valid (must be val (default) or prod)" ;
else
    # confirm that the version is specified
    if [[ -z "${VERSION}" ]]; then
        echo "ERROR: Please set `VERSION` variable in .env file."
    # if all checks pass, execute deployment
    # create temporary local version of setup.py and delete after creating wheel
    else
        # set upload location
        if [[ "${ENVIRON}" =~ ^(e2val)$ ]]; then 
            UPLOAD=uat_val_catalog/dqm_conv/dqm_package_volume_val;
        elif [[ "${ENVIRON}" =~ ^(e2prod)$ ]]; then 
            UPLOAD=cms_prod_catalog/dqm_conv/dqm_package_volume_prod;
        fi
        echo "Deploying (VERSION=${VERSION}) ..."
        cd dqm/batch
        python reverse_lookup.py
        cd ../..
        rm -rf build/*
        rm -rf *.egg-info/*
        python create_setup_local.py
        python setup_local.py bdist_wheel
        rm -rf setup_local.py
        echo "databricks --profile ${ENVIRON} fs cp ./dist/dqm-${VERSION}-py3-none-any.whl dbfs:/Volumes/${UPLOAD}/dqm-${VERSION}-py3-none-any.whl --overwrite"
        databricks --profile ${ENVIRON} fs cp ./dist/dqm-${VERSION}-py3-none-any.whl dbfs:/Volumes/${UPLOAD}/dqm-${VERSION}-py3-none-any.whl --overwrite
    fi
fi