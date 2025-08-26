#!groovy
def err = null
withEnv([
        'PROJECT=pipelinewise',
        'PRODUCT=pipelinewise',
        'CI_SKIP=false',
        'PUBLISH_IMAGE=false',
        'MAIL_RECIPIENTS=trimble-tl-itops-ug@trimble.com',
        'DEPLOY=true'
        ]) {

    node('dockerindocker') {
        try {
            def app

            properties([
                [
                    $class  : 'jenkins.model.BuildDiscarderProperty',
                    strategy: [
                        $class      : 'LogRotator',
                        numToKeepStr: '10'
                    ]
                ]
            ])

            stage('Checkout repository') {
                checkout scm
                result = sh (script: "git log -1 | grep '.*\\[ci skip\\].*'", returnStatus: true)
                if (result == 0) {
                    CI_SKIP = "true"
                    error "'[ci skip]' found in git commit message. Aborting."
                }
            }

            stage('Set variables') {
                /* Workaround when picking branch to build in Jenkins UI */
                if (env.BRANCH_NAME == null) {
                    branchName = sh (
                            script: 'git rev-parse --abbrev-ref HEAD',
                            returnStdout: true
                        ).trim()
                } else {
                    branchName = env.BRANCH_NAME
                }

                echo "Taking decisions based on git branch (${branchName}) ..."

                if (branchName == 'master') {
                    PUBLISH_IMAGE = 'true'
                } else {
                    PUBLISH_IMAGE = env.PUBLISH_ARTIFACTS
                }
            }

            stage('Build image') {
                echo "Get version from setup.py"
                //get verison
                VERSION = sh (script: "grep version setup.py | tr -dc '0-9.'", returnStdout: true).trim()

                echo "version we will build: ${VERSION} "
                writeFile file: "version.txt", text: "${VERSION}"

                app = docker.build("${env.PRODUCT}:${VERSION}")
            }


            if (DEPLOY == 'true' && branchName == 'master') {
                stage('Push image') {
                    docker.withRegistry('https://844654352679.dkr.ecr.eu-west-1.amazonaws.com') {
                        app.push("${env.BUILD_NUMBER}")
                        app.push("${VERSION}")
                        app.push("${VERSION}-${env.BUILD_NUMBER}")
                        app.push("${VERSION}-latest")
                        app.push("latest")
                    }
                }
            }
            currentBuild.result = "SUCCESS"
        } catch (caughtError) {
            err = caughtError
            currentBuild.result = "FAILURE"
        } finally {
            cleanupImage("${PRODUCT}", "${VERSION}")
            if (CI_SKIP == "true") {
                currentBuild.result = 'NOT_BUILT'
            }
            if (currentBuild.result != "ABORTED") {
                /* Send e-mail notifications for failed or unstable builds.
                   currentBuild.result must be non-null for this step to work. */
                step([$class                  : 'Mailer',
                      notifyEveryUnstableBuild: true,
                      recipients              : "${env.MAIL_RECIPIENTS}",
                      sendToIndividuals       : true])

                /* Must re-throw exception to propagate error */
                if (err) {
                    throw err
                }

            }
        }
    }
}
