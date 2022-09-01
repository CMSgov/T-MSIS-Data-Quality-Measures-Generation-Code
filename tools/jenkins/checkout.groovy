private def gitCheckout(String repoUrl, String credential, String refname) {
    checkout([
        $class: 'GitSCM',
        branches: [[name: "$refname"]],
        userRemoteConfigs: [[
            url: repoUrl,
            credentialsId: credential,
        ]]
    ])
}

def checkout_dq_measures_python(String refname) {
    // https://jenkins.macbisdw.cmscloud.local/credentials/store/system/domain/_/credential/aaeef4d9-ad18-4694-8e3f-29f9a2a0baeb/
    credentialsId = 'aaeef4d9-ad18-4694-8e3f-29f9a2a0baeb'
    gitCheckout('git@github.com:tmsis/dq_measures_python', credentialsId, refname)
}

return this