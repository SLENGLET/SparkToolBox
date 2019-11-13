pipeline {
    environment {
        BUILD_SCRIPTS_GIT="https://github.com/SLENGLET/SparkToolBox.git"
        BUILD_SCRIPTS='mypipeline'
        WORKSPACE='/var/jenkins_home/workspace/test-pipeline'
    }
    
    agent any

    stages {
        stage('Infos Env') {
            steps {
                sh "echo 'Running ${env.BUILD_ID} on ${env.JENKINS_URL}'"
                sh "rm -rf $WORKSPACE/repo"
            }
        }
        stage('Checkout: Code') {
            steps {
                sh "echo 'Checkout'"
                sh "mkdir -p $WORKSPACE/repo"
                sh "git clone $BUILD_SCRIPTS_GIT repo/$BUILD_SCRIPTS"
                sh "chmod -R +x $WORKSPACE/repo/$BUILD_SCRIPTS"
            }
        }
        stage('Test : Junit + Cucumber') {
            steps {
                echo 'Build Test : Junit + Cucumber'
                sh "mvn clean test -Dtest=**/* -DfailIfNoTests=false -e"
            }
        }
        stage('Build Install') {
            steps {
                echo 'Build install uber jar'
                sh "mvn install"
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
}