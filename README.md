# custom_glue_connector_example

An expanded custom glue connector example

## Reasoning

Custom AWS Glue connectors are not well-documented.  [aws-glue-samples](https://github.com/aws-samples/aws-glue-samples/) has examples and can get you started, but in practice I found
that I wanted something more.  The starting point for this code is [SparkConnectorCSV.java](https://github.com/aws-samples/aws-glue-samples/blob/master/GlueCustomConnectors/development/Spark/SparkConnectorCSV.java). This is not production-ready code; rather this is a simple example along with findings that will hopefully help anyone dabbling with this feature.  It should be
noted that there is an s3 connector available for use in Glue Jobs, and you should absolutely use that instead of writing your own.  However, I think using s3 as a datasource for learning
the ins and outs of Custom Connectors is a great starting point, since the amount of setup you need to do for source/target data sources is minimal.

## Build
- Clone repo
- Build project using Maven:
  - Ensure you are using Java 8, as this is the version of Java the Glue Spark Runtime uses.
  
## Deploy






