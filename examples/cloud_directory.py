"""
Simple Demo for AWS Cloud Directory Service

Assume schema has been uploaded and applied, cloud directory has been created.
"""
import aiobotocore
import asyncio
import os


os.environ["AWS_PROFILE"] = "YOUR_AWS_PROFILE"
loop = asyncio.get_event_loop()


async def test_cloud_directory():
    # setup aws session and client
    session = aiobotocore.get_session()
    cloud_directory_client = session.create_client("clouddirectory")

    # List Directories
    resp = await cloud_directory_client.list_directories(state="ENABLED")
    directories = resp["Directories"]
    directory = directories[0]
    directory_name = directory["Name"]
    directory_arn = directory["DirectoryArn"]

    # Get Directories
    resp = await cloud_directory_client.get_directory(DirectoryArn=directory_arn)
    
    # List Published SchemaArns
    resp = await cloud_directory_client.list_published_schema_arns()
    schema_arns = resp["SchemaArns"]
    schema_arn = schema_arns[0]

    # List Facet Names
    resp = await cloud_directory_client.list_facet_names(SchemaArn=schema_arn)
    facet_names = resp["FacetNames"]

    # List Facet Attributes
    resp = await cloud_directory_client.list_facet_attributes(SchemaArn=schema_arn, Name="Directory")

    # List Applied SchemaArns
    resp = await cloud_directory_client.list_applied_schema_arns(DirectoryArn=directory_arn)
    applied_schema_arn = resp["SchemaArns"][0]

    # Create Directory Object
    resp = await cloud_directory_client.create_object(
                    DirectoryArn=directory_arn,
                    SchemaFacets=[
                        {
                            "SchemaArn": applied_schema_arn,
                            "FacetName": "Directory",
                        },
                    ],
                    ObjectAttributeList=[
                        {
                            "Key": {
                                "SchemaArn": applied_schema_arn,
                                "FacetName": "Directory",
                                "Name": "Name",
                            },
                            "Value": {
                                "StringValue": "US", 
                            },
                        },
                    ],
                     ParentReference={
                        "Selector": "/"
                    },
                    LinkName="US",
                )

    resp = await cloud_directory_client.create_object(
                    DirectoryArn=directory_arn,
                    SchemaFacets=[
                        {
                            "SchemaArn": applied_schema_arn,
                            "FacetName": "Directory",
                        },
                    ],
                    ObjectAttributeList=[
                        {
                            "Key": {
                                "SchemaArn": applied_schema_arn,
                                "FacetName": "Directory",
                                "Name": "Name",
                            },
                            "Value": {
                                "StringValue": "US.CA", 
                            },
                        },
                    ],
                    ParentReference={
                        "Selector": "/US/"
                    },
                    LinkName="CA",
                )

    # Create File Object
    resp = await cloud_directory_client.create_object(
                    DirectoryArn=directory_arn,
                    SchemaFacets=[
                        {
                            "SchemaArn": applied_schema_arn,
                            "FacetName": "File",
                        },
                    ],
                    ObjectAttributeList=[
                        {
                            "Key": {
                                "SchemaArn": applied_schema_arn,
                                "FacetName": "File",
                                "Name": "Name",
                            },
                            "Value": {
                                "StringValue": "US.CA.SF", 
                            },
                        },
                        {
                            "Key": {
                                "SchemaArn": applied_schema_arn,
                                "FacetName": "File",
                                "Name": "MetricAddress",
                            },
                            "Value": {
                                "StringValue": "aws-random-com", 
                            },
                        },
                    ],
                    ParentReference={
                        "Selector": "/US/CA"
                    },
                    LinkName="SF",
                )
    
    # Get Metric US.CA.SF
    import time
    for i in range(10):
        start = time.time()
        resp = await cloud_directory_client.get_object_information(
                    DirectoryArn=directory_arn,
                    ObjectReference={
                        "Selector": "/US/CA/SF",
                    },
                    ConsistencyLevel="EVENTUAL",
                )
        resp = await cloud_directory_client.get_object_attributes(
                    DirectoryArn=directory_arn,
                    ObjectReference={
                        "Selector": "/US/CA/SF",
                    },
                    ConsistencyLevel="EVENTUAL",
                    SchemaFacet={
                        "SchemaArn": applied_schema_arn,
                        "FacetName": "File",
                    },
                    AttributeNames=[
                        "MetricAddress",
                        "Name",
                    ]
                )
        end = time.time()
        print("[ Retrieving Metric, takes %s sec]" % str(end - start))

    # tear down aws session and client
    await cloud_directory_client.close()

def main():
    loop.run_until_complete(test_cloud_directory())

main()

# Latency numbers for get_object_information + get_object_attributes
# $ python3 aws_cloud_directory.py
# [ Retrieving Metric, takes 0.06780076026916504 sec]
# [ Retrieving Metric, takes 0.06684064865112305 sec]
# [ Retrieving Metric, takes 0.07030677795410156 sec]
# [ Retrieving Metric, takes 0.06939697265625 sec]
# [ Retrieving Metric, takes 0.07062506675720215 sec]
# [ Retrieving Metric, takes 0.07235836982727051 sec]
# [ Retrieving Metric, takes 0.06737804412841797 sec]
# [ Retrieving Metric, takes 0.07101607322692871 sec]
# [ Retrieving Metric, takes 0.06745386123657227 sec]
# [ Retrieving Metric, takes 0.06923484802246094 sec]
