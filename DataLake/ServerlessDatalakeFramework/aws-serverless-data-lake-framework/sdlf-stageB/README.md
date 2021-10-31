# StageB State Machine
This repository contains the code for the StageB State Machine. The CloudFormation stack in this repository is deployed by the CICD pipeline created by the **PIPELINE** stack, and thus should not be deployed manually.

## Architecture
The diagram below illustrates the high-level architecture of the resources deployed by this template:

![StageB Architecture](docs/stageBStateMachine.png)

This template creates the following resources:
1. Lambda functions
2. State Machine
3. IAM roles
4. IAM managed policies
5. CloudWatch Alarms

## Resources Deployed
In detail, this template deploys the following resources:
1. IAM
   1. `rRoleLambdaExecutionQueueRoutingStep`
   2. `rRoleLambdaExecutionStep1`
   3. `rRoleLambdaExecutionStep2`
   4. `rRoleLambdaExecutionStep3`
   5. `rRoleLambdaExecutionErrorStep`
2. Lambda
   1. `rLambdaRoutingStep`
   2. `rLambdaRedrive`
   3. `rLambdaStep1`
   4. `rLambdaStep2`
   5. `rLambdaStep3`
   6. `rLambdaErrorStep`
3. CloudWatch Alarms
   1. `rLambdaErrorStepCloudWatchAlarm`
   2. `rLambdaRoutingStepCloudWatchAlarm`
4. State Machine
   1. `rStateMachine`

Optional (i.e. Elasticsearch Enabled):
1. Log Groups (one per Lambda Function)
2. UpdateSubscriptionFilters (one per Lambda Function)

## Deployment
This stack should **not** be deployed manually. CodePipeline will trigger upon any `git push` operation to the repository containing this stack, releasing changes automatically. All parameters are passed into this stack through CodePipeline from the **PIPELINE** stack.
