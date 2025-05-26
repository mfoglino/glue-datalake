

### 1. Generate AWS Resources
To generate the necessary AWS resources, run the following command in your terminal:

```#:>  cd terraform```
```#:>  terraform apply```


### 2. Generate DATA for examples

Inside the Docker Terminal:

```#:> WORKSPACE_LOCATION=/Users/marcos.foglino/workspace/learning/glue-datalake``` 
```#:> pytest -s tests/test_generate_test_table.py::test_generate_initial_schema```
```#:> pytest -s tests/test_generate_test_table.py::test_evolve_schema```


### Running the solution

Go to the step function 'marcos-datalake-orchestration' in the AWS console and trigger the execution of the step function with the following input:

```
{
    "timestamp_bookmark_str": "None"
}
```



### Run Project in Visual Code


In the Bash / ZSH terminal, run the following command to start the Docker container:

```
#> WORKSPACE_LOCATION=/Users/marcos.foglino/workspace/learning/glue-datalake
#> docker run -it --rm  -v ~/.aws:/home/hadoop/.aws -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ -e AWS_PROFILE=caylent-dev-test -e ENVIRONMENT=dev -e AWS_REGION=us-east-1 -e AWS_DEFAULT_REGION=us-east-1 -p 4040:4040 -p 18080:18080  --name glue5_pyspark public.ecr.aws/glue/aws-glue-libs:5 pyspark
```

Inside the container shell:

Before to run any piece of python code:
```
PYTHONPATH=$PYTHONPATH:/home/hadoop/workspace
```

Examples:
```
python3 glue_scripts/job_raw_to_stage.py  --table core_program --timestamp_bookmark_str '2025-01-01 00:00:00.000' --raw_bucket_name marcos-test-datalake-raw --raw_bucket_prefix tables --bookmark_table raw_to_stage_bookmarks  --JOB_NAME rawtest 
pytest -s tests/test_landing_to_raw.py
 ```


Visual Code:
============

In Preferences: Open Workspace Settings (JSON), and put this json:

{
    "python.defaultInterpreterPath": "/usr/bin/python3.11",
    "python.analysis.extraPaths": [
        "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/lib/spark/python/:/usr/lib/spark/python/lib/",
    ]
}


