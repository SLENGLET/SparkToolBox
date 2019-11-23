pipeline {
    environment {
        BUILD_SCRIPTS_GIT="https://github.com/SLENGLET/SparkToolBox.git"
        BUILD_SCRIPTS='/'
        WORKSPACE='/var/jenkins_home/workspace/test-pipeline'
    }

    agent any

    stages {
        stage('Infos Env') {
            steps {
                sh "echo 'Running ${env.BUILD_ID} on ${env.JENKINS_URL}'"
                //sh "rm -rf $WORKSPACE/*"
            }
        }
        stage('Checkout: Code') {
            steps {
                sh "echo 'Checkout'"
                sh "mkdir -p $WORKSPACE"
                sh "git clone $BUILD_SCRIPTS_GIT $WORKSPACE"
                sh "chmod -R +x $WORKSPACE"

                //sh "cd $WORKSPACE/repo/$BUILD_SCRIPTS"
            }
        }
        stage('Compile') {
            steps {
                echo 'Compile'

                sh "mvn compile"
            }
        }
        stage('Test : Junit + Cucumber') {
            steps {
                echo 'Build Test : Junit + Cucumber'

                sh "mvn test -Dtest=**/* -DfailIfNoTests=false -e"
            }
        }
        stage('Build Install') {
            steps {
                echo 'Build install uber jar'
                sh "mvn clean install"
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
}