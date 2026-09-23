# Debezium Kafka Connect for the AWS showcase: the same image the local plane
# runs, plus the aws-msk-iam-auth jar so the worker (and its embedded
# producer/consumer) can authenticate to MSK Serverless with AWS_MSK_IAM.
# Build from the REPO ROOT and push to the modules/ecs_connect ECR repo:
#   docker build -f cdc/connect/Dockerfile -t <ecr>/harbormaster-base-cdc-connect:latest .
FROM quay.io/debezium/connect:2.7

ENV MSK_IAM_AUTH_VERSION=2.2.0
RUN curl -fsSL -o /kafka/libs/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar \
    https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar
