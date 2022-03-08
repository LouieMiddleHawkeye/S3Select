#!/usr/bin/env groovy
library identifier: 'jenkins-shared-pipeline@master', retriever: modernSCM(
  [$class: 'GitSCMSource',
   remote: 'https://github.com/HawkEyeInnovations/jenkins-shared-pipeline',
   credentialsId: 'CIBotGithub'])

commonPipeline()