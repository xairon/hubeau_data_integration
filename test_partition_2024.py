"""
Test script to trigger ecoulement job with partition 2024
This will help us verify if the date filtering works correctly
"""
import requests
import json

# Dagster GraphQL endpoint
DAGSTER_URL = "http://localhost:8080/graphql"

# GraphQL mutation to launch a job run with a partition
mutation = """
mutation LaunchPartitionBackfill($partitionSetName: String!, $partitionNames: [String!]!) {
  launchPartitionBackfill(
    backfillParams: {
      partitionSetName: $partitionSetName
      partitionNames: $partitionNames
      selector: {
        partitionSetName: $partitionSetName
      }
    }
  ) {
    __typename
    ... on LaunchBackfillSuccess {
      backfillId
    }
    ... on PartitionSetNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

# Try simpler approach - launch job run with partition key
simple_mutation = """
mutation LaunchRun($jobName: String!, $partitionKey: String!) {
  launchRun(
    executionParams: {
      selector: {
        jobName: $jobName
        repositoryLocationName: "hubeau_pipeline"
        repositoryName: "__repository__"
      }
      runConfigData: "{}"
      mode: "default"
    }
    partition: $partitionKey
  ) {
    __typename
    ... on LaunchRunSuccess {
      run {
        runId
        status
      }
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""

print("Triggering ecoulement job with partition 2024...")

variables = {
    "jobName": "ecoulement",
    "partitionKey": "2024"
}

try:
    response = requests.post(
        DAGSTER_URL,
        json={
            "query": simple_mutation,
            "variables": variables
        },
        headers={"Content-Type": "application/json"}
    )

    response.raise_for_status()
    result = response.json()

    print("\nResponse:")
    print(json.dumps(result, indent=2))

    if 'errors' in result:
        print("\nGraphQL Errors:")
        for error in result['errors']:
            print(f"  - {error.get('message', error)}")

    if 'data' in result and 'launchRun' in result['data']:
        launch_result = result['data']['launchRun']
        if launch_result.get('__typename') == 'LaunchRunSuccess':
            run_id = launch_result['run']['runId']
            print(f"\nJob launched successfully!")
            print(f"Run ID: {run_id}")
            print(f"\nCheck logs: http://localhost:8080/runs/{run_id}")
        else:
            print(f"\nFailed to launch: {launch_result}")

except Exception as e:
    print(f"\nError: {e}")
    print(f"Response status: {response.status_code if 'response' in locals() else 'N/A'}")
    if 'response' in locals():
        print(f"Response text: {response.text}")
