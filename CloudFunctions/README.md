Nexmatix Cloud Functions
========================

To install the cloud functions:

    cd CloudFunctions/config
    gcloud beta functions deploy manifold-configuration-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-configuration

    cd CloudFunctions/state
    gcloud beta functions deploy manifold-state-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-state
