

### 1. Generate AWS Resources
To generate the necessary AWS resources, run the following command in your terminal:

```#:>  cd terraform```
```#:>  terraform apply```


### 2. Generate DATA for examples

Inside the Docker Terminal:

```#:> WORKSPACE_LOCATION=/Users/marcos.foglino/workspace/learning/glue-datalake-example-iceberg``` 
```#:> pytest -s tests/test_generate_test_table.py::test_generate_initial_schema```
```#:> pytest -s tests/test_generate_test_table.py::test_evolve_schema```


### Running the solution

Go to the step function 'marcos-datalake-orchestration' in the AWS console and trigger the execution of the step function with the following input:

```
{
    "timestamp_bookmark_str": "None"
}
```



### Run code/tests in Docker Image

To run the code and tests in a Docker container, follow these steps:

1. Run the Docker for Glue: In your local machine, go to Bash / ZSH terminal, run the following command to start the Docker container:

```
#> WORKSPACE_LOCATION=/Users/marcos.foglino/workspace/learning/glue-datalake-example-iceberg
#> docker run -it --rm  -v ~/.aws:/home/hadoop/.aws -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ -e AWS_PROFILE=caylent-dev-test -e ENVIRONMENT=dev -e AWS_REGION=us-east-1 -e AWS_DEFAULT_REGION=us-east-1 -p 4040:4040 -p 18080:18080  --name glue5_pyspark public.ecr.aws/glue/aws-glue-libs:5 pyspark
```

2. Go to Docker Desktop and check that the container is running. You should see the container named `glue5_pyspark`.
3. In Docker Desktop go to **glue5_spark** container -> open terminal
4.Inside the container shell: Before to run any piece of python code, run:
```
export PYTHONPATH=$PYTHONPATH:/home/hadoop/workspace
```

Examples:
```
pytest -s tests/test_landing_to_raw.py::test_clean_solution

python3 glue_scripts/job_raw_to_stage.py  --table core_program --timestamp_bookmark_str '2025-01-01 00:00:00.000' --raw_bucket_name marcos-test-datalake-raw --raw_bucket_prefix tables --bookmark_table landing_to_raw_bookmark  --JOB_NAME rawtest 

pytest -s tests/test_landing_to_raw.py
 ```

### Building Python Artifact
```
python setup.py bdist_wheel --dist-dir /tmp/iceberg_test;  rm -rf build; aws s3 cp /tmp/iceberg_test/python_libs-0.1.0-py3-none-any.whl  s3://marcos-test-datalake-glue-scripts/artifacts/
```


### (Optional) Attach Docker to Visual Code:

In Preferences: Open Workspace Settings (JSON), and put this json:

{
    "python.defaultInterpreterPath": "/usr/bin/python3.11",
    "python.analysis.extraPaths": [
        "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/lib/spark/python/:/usr/lib/spark/python/lib/",
    ]
}


### Notes
- Glue bookmarks are not supported in the Glue Docker (public.ecr.aws/glue/aws-glue-libs:5)
- Lake Formation working properly is very important. Otherwise you can get unuseful errors. Like table for example 'stage.people_table not found.', 
when the real error is related to LakeFormation permissions.
