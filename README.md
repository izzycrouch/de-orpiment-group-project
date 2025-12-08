# Orpiment Group Project

## Overview

In this project, we have created applications that will extract, transform, and load data from the totesys database into a data lake and warehouse hosted in AWS. We have used Terraform to manage the AWS services used.

## Objectives

- Extract raw data from the totesys database using a Lambda Function and store it in an S3 bucket

- Clean and standardise the raw data using a Lambda Function, and store this cleaned data in another S3 bucket

- Transform the data into a 'star' schema using another Lambda Function that will have fact and dimension tables 

- Store the transformed data as parquet files in another S3 bucket

- Create a database with the transformed data to act as the data warehouse (which will contain a full history of all updates to the facts tables)

- Automate the pipeline by creating a Step Function and adding a job scheduler using Eventbridge trigger the Lambda Functions

- Log progress of the pipeline using Cloudwatch

- Create a visual presentation that allows users to view useful data in the warehouse


## How to run
1. Clone repo
2. Run `make all` to create and activate virtual environment and download required packages and libraries
3. Run `pytest` and check all tests pass
4. Go into the terraform folder in your terminal and run `terraform init`
5. Run `terraform plan`
6. Run `terraform apply`