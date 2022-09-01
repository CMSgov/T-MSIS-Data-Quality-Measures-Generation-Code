def notifySlack(String message = '') {
    // Send a build status notification to Slack.

    // We only want to notify for the master branch, not for pull request builds.
    if (env.BRANCH_NAME != 'master') {
        return
    }

    def slack = load 'tools/jenkins/slack.groovy'
    def channel = "dc-alerts" 
    
    slack.notifySlack(channel, message)
}

pipeline {
    agent { 
        dockerfile {
            args '-v $HOME/.aws:/root/.aws'
        }
    }
    parameters {
        string(
            name: 'DQM_REFNAME',
            description: 'A git hash or branch name from the https://github.com/tmsis/dq_measures_python repo.',
            defaultValue: 'master')
    }

    options {
        parallelsAlwaysFailFast()
        timestamps()
        disableConcurrentBuilds() // we only want one build at a time for this environment
        copyArtifactPermission('*')
    }

    stages {
        stage('Build') {
            steps {
                    sh 'python setup.py bdist_wheel'
            }
        }
        stage('Upload to S3') {
            steps {
                    sh 'aws s3 cp dist s3://aws-hhs-cms-cmcs-databricks-dev/databricks-macbis-dev/0/FileStore/shared_uploads/dqm/ --recursive'
                    sh 'aws s3 cp dist s3://aws-hhs-cms-cmcs-databricks-val/databricks-macbis-val/0/FileStore/shared_uploads/dqm/ --recursive'
                    sh 'aws s3 cp dist s3://aws-hhs-cms-cmcs-databricks-prod/databricks-macbis-prod/0/FileStore/shared_uploads/dqm/ --recursive'
            }
        }
    }
    post {
        always {
            // we always want to know what's going on with this job
            notifySlack("dq_measures_python: ${DQM_REFNAME}")
        }
    }
}