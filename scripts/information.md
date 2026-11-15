# **DEV Environment**

## **S3 buckets** 

    bronze_bucket_name:         yt-analysis-pipeline-bronze-us-east-1-dev
    silver_bucket_name:         yt-analysis-pipeline-silver-us-east-1-dev
    gold_bucket_name:           yt-analysis-pipeline-gold-us-east-1-dev
    script_bucket_name:         yt-analysis-pipeline-script-us-east-1-dev

## **SNS Alert**

    SNS_ARN:                    arn:aws:sns:us-east-1:502244072893:yt-data-analysis-pipeline-alerts-dev

## **AWS Glue Databases**

    bronze_db:                  yt-data-analysis-pipeline-bronze-db-dev
    silver_db:                  yt-data-analysis-pipeline-silver-db-dev
    gold_db:                    yt-data-analysis-pipeline-gold-db-dev

## **AWS Glue Tables**

    bronze_tables:              raw_statistics & raw_statistics_reference_data
    silver_tables:              clean_statistics & clean_reference_data
    gold_tables:                 

## **Glue job parameter**

**bronze_to_silver_statistics**

    --bronze_database           yt-data-analysis-pipeline-bronze-db-dev 
    --bronze_table              raw_statistics
    --silver_bucket             yt-analysis-pipeline-silver-us-east-1-dev   
    --silver_database           yt-data-analysis-pipeline-silver-db-dev  
    --silver_table              clean_statistics 
       
**silver_to_gold_analytics**

    --silver_database           yt-data-analysis-pipeline-silver-db-dev
    --gold_bucket               yt-analysis-pipeline-gold-us-east-1-dev
    --gold_database             yt-data-analysis-pipeline-gold-db-dev

## **ARN**
    INGESTION_LAMBDA_ARN	    arn:aws:lambda:us-east-1:502244072893:function:yt-data-analysis-pipeline-lambda-youtube-api-ingestion-dev
    JSON_TRANSFORM_LAMBDA_ARN	arn:aws:lambda:us-east-1:502244072893:function:yt-data-analysis-pipeline-lambda-json-to-parquet-dev
    DATA_QUALITY_LAMBDA_ARN	    arn:aws:lambda:us-east-1:502244072893:function:yt-data-analysis-pipeline-lambda-data-quality-check-dev
    SILVER_GLUE_JOB_NAME	    yt-data-analysis-pipeline-bronze-to-silver-statistics
    GOLD_GLUE_JOB_NAME	        yt-data-analysis-pipeline-silver-to-gold-analytics
    SNS_TOPIC_ARN	            SNS_ARN: arn:aws:sns:us-east-1:502244072893:yt-data-analysis-pipeline-alerts-dev

# **PROD Environment**

## **S3 buckets** 

    bronze_bucket_name:         yt-analysis-pipeline-bronze-us-east-1-prod
    silver_bucket_name:         yt-analysis-pipeline-silver-us-east-1-prod
    gold_bucket_name:           yt-analysis-pipeline-gold-us-east-1-prod
    script_bucket_name:         yt-analysis-pipeline-script-us-east-1-prod

## **SNS Alert**

    SNS_ARN:                    arn:aws:sns:us-east-1:502244072893:yt-data-analysis-pipeline-alerts-prod

## **AWS Glue Databases**

    bronze_db:                  yt-data-analysis-pipeline-bronze-db-prod
    silver_db:                  yt-data-analysis-pipeline-silver-db-prod
    gold_db:                    yt-data-analysis-pipeline-gold-db-prod

## **AWS Glue Tables**

    bronze_tables:              raw_statistics & raw_statistics_reference_data
    silver_tables:              clean_statistics & clean_reference_data
    gold_tables:                 

## **Glue job parameter**

**bronze_to_silver_statistics**

    --bronze_database           yt-data-analysis-pipeline-bronze-db-prod 
    --bronze_table              raw_statistics
    --silver_bucket             yt-analysis-pipeline-silver-us-east-1-prod   
    --silver_database           yt-data-analysis-pipeline-silver-db-prod  
    --silver_table              clean_statistics 
       
**silver_to_gold_analytics**

    --silver_database           yt-data-analysis-pipeline-silver-db-prod
    --gold_bucket               yt-analysis-pipeline-gold-us-east-1-prod
    --gold_database             yt-data-analysis-pipeline-gold-db-prod

## **ARN**
    INGESTION_LAMBDA_ARN	    arn:aws:lambda:us-east-1:502244072893:function:yt-data-analysis-pipeline-lambda-youtube-api-ingestion-prod
    JSON_TRANSFORM_LAMBDA_ARN	arn:aws:lambda:us-east-1:502244072893:function:yt-data-analysis-pipeline-lambda-json-to-parquet-prod
    DATA_QUALITY_LAMBDA_ARN	    arn:aws:lambda:us-east-1:502244072893:function:yt-data-analysis-pipeline-lambda-data-quality-check-prod
    SILVER_GLUE_JOB_NAME	    yt-data-analysis-pipeline-bronze-to-silver-statistics
    GOLD_GLUE_JOB_NAME	        yt-data-analysis-pipeline-silver-to-gold-analytics
    SNS_TOPIC_ARN	            SNS_ARN: arn:aws:sns:us-east-1:502244072893:yt-data-analysis-pipeline-alerts-prod