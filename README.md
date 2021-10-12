
# ADBE  Daily Revenue - AWS Serverless ETL CodePipeline


This is a sample application that you can modify to create a Continuous Delivery / Continuous Deployment (CD) Pipeline which will setup the ETL Pipeline for processing Daily Revenue Data using resources like [AWS CodePipeline](https://aws.amazon.com/codepipeline/), [AWs Lambda](https://aws.amazon.com/lambda/), [AWS Glue](https://aws.amazon.com/glue/), [AWS Cloudformation](https://aws.amazon.com/cloudformation/) and [GitHub](https://github.com/) as a source code repository.

CD Pipelines define the application to be deployed using an ["Infrastructure as Code"](https://en.wikipedia.org/wiki/Infrastructure_as_code) (IaC) strategy, and in our case we use [AWS CloudFormation](https://aws.amazon.com/cloudformation/) as our IaC tool. The application or resources can be deployed using AWS Code Pipeline using GitHub Webhooks.

The definition of the application to be deployed can be found in the [`CFtemplate`](https://github.com/guptaabhi07/ADBE_Assessment/tree/main/aws_cloudformation) template.

Moreover the *pipeline itself* is defined in code, also using CloudFormation, and its definition is the [`Pipeline`](https://github.com/guptaabhi07/ADBE_Assessment/tree/main/aws_cloudformation) template. The template contains the aws resources definition for lambda, glue, crawler and associated alarms.

We are trying to define the pipeline in the same repository as the application itself - this is possible since AWS CodePipeline provides *CD as a service* - we do not need to manage any pipeline servers. Keeping the CD definition closely tied to the application definition allows us to make infrastructural changes quickly and simply.

GitHub is an extremely popular location to host source code for applications, and this example uses GitHub as its source code location. We define the integration between AWS and GitHub solely within the Pipeline's template - no manual use of the AWS Console or CodePipeline API are required.

As part of the GitHub integration we make use of CodePipeline's ["GitHub Webhook"](https://docs.aws.amazon.com/codepipeline/latest/userguide/pipelines-webhooks.html) feature to allow for executions that trigger more quickly, and that allow source event filtering.

![Design](https://github.com/guptaabhi07/ADBE_Assessment/blob/main/aws_images/ETL%20Design.jpg "Pipeline Design")

## How to create the pipeline

1. Fork this repository to your own GitHub repository

2. Create a new GitHub personal access token for this application. See [here](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) for how to do this - CodePipeline needs just the `repo` scope permissions. I recommend you name the token for this particular pipeline, at least to get started, and that you store the token somewhere safe, like a password manager.

3. :warning: The user associated with the personal access token above **MUST** have administrative rights for the Github repo - either by being an owner of the repo, or having been granted admin privs. Simply having write access is not sufficient, because this template attempts to create a webhook in Github. If your user has insufficient privileges the pipeline creation process will fail, but will create an stranded / undeletable version of your application stack.

4. Now, create a CodePipeline in AWS Console with relevant name. Make sure you create a proper IAM role with codepipeline as the trusted entity to assume the role. Pls dont select the option of create IAM role while creating the codepipeline.

5. In the CodePipeline, also make sure you have an IAM role for Cloudformation with associated relevant permission for resource creation and update.

6. Fill all the details in the Codepipeline with your Github repository and webhook on the cloudformation template .

7. Run the CodePipeline and release the changes now.

Once you've run Create Stack command then watch both the CloudFormation and then CodePipeline consoles to evaluate whether the process has been successful. You should have one new CloudFormation stacks - for all the resources lambda, glue and crawler.

To test everything, after the pipeline has successfully completed it's first run, trigger the pipeline by putting a relevant daily revenue file in the s3 bucket.

![Pipeline Deployment](https://github.com/guptaabhi07/ADBE_Assessment/blob/main/aws_images/CodePipeline.png "Pipeline Deployment")

## Dashboard

Once you haverun the pipeline successfully, you should be able to see the analysis for the daily Revenue based on Host and Keyword Search on the QuickSight in below fashion manner, depending how and what type of analysis you have done.

![QuickSight Dashboard](https://github.com/guptaabhi07/ADBE_Assessment/blob/main/aws_images/QuickSight%20Analysis.png "Analysis")

## How to update the pipeline

When you need to update the application code or structure as defined in the Cloudformation template, then simply pushing your changes to source control will be sufficient - CodePipeline references these files from source on every pipeline run.


## Teardown

:warning: Run these steps **in order**, otherwise you may end up with stranded resources:

1. Delete the application stack through the Cloudformation Console.
2. Check the S3 Bucket and delete them manually for now.

## Questions, Comments, Additions, Suggestions

If you have any questions, comments, additions or suggestions please feel free to comment through GitHub.

