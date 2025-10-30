#!/usr/bin/env python3
"""Trigger piezometry_chroniques_csv via Dagster GraphQL API"""

import requests
import json
import time
import sys

DAGSTER_URL = "http://localhost:8080/graphql"

# Step 1: Get workspace info to find correct names
print("=" * 80)
print("STEP 1: Getting workspace information...")
print("=" * 80)

query_workspace = """
query {
  workspaceOrError {
    ... on Workspace {
      locationEntries {
        name
        locationOrLoadError {
          ... on RepositoryLocation {
            name
            repositories {
              name
              pipelines {
                name
              }
            }
          }
        }
      }
    }
  }
}
"""

response = requests.post(DAGSTER_URL, json={"query": query_workspace})
print("Workspace response:", json.dumps(response.json(), indent=2))

# Step 2: Launch the asset materialization
print("\n" + "=" * 80)
print("STEP 2: Launching piezometry_chroniques_csv materialization...")
print("=" * 80)

# Updated GraphQL mutation for asset materialization
mutation_launch = """
mutation LaunchAssetMaterialization {
  launchRun(
    executionParams: {
      selector: {
        repositoryLocationName: "hubeau_pipeline"
        repositoryName: "__repository__"
        pipelineName: "__ASSET_JOB"
        assetSelection: [
          { path: ["piezometry_chroniques_csv"] }
        ]
      }
      runConfigData: "{}"
    }
  ) {
    __typename
    ... on LaunchRunSuccess {
      run {
        runId
        status
        pipelineName
      }
    }
    ... on InvalidSubsetError {
      message
    }
    ... on PipelineNotFoundError {
      message
    }
    ... on RunConfigValidationInvalid {
      errors {
        message
      }
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""

response = requests.post(DAGSTER_URL, json={"query": mutation_launch})
result = response.json()

print("Launch response:", json.dumps(result, indent=2))

# Check if successful
if result.get("data", {}).get("launchRun", {}).get("__typename") == "LaunchRunSuccess":
    run_id = result["data"]["launchRun"]["run"]["runId"]
    print(f"\n✅ Run launched successfully!")
    print(f"Run ID: {run_id}")
    print(f"View at: http://localhost:8080/runs/{run_id}")

    # Step 3: Poll for status
    print("\n" + "=" * 80)
    print("STEP 3: Monitoring execution...")
    print("=" * 80)

    query_status = """
    query GetRunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          stats {
            __typename
            ... on RunStatsSnapshot {
              stepsSucceeded
              stepsFailed
            }
          }
        }
      }
    }
    """

    for i in range(60):  # Poll for up to 5 minutes
        time.sleep(5)
        response = requests.post(DAGSTER_URL, json={
            "query": query_status,
            "variables": {"runId": run_id}
        })
        status_data = response.json()

        run_status = status_data.get("data", {}).get("runOrError", {}).get("status")
        print(f"[{i*5}s] Status: {run_status}")

        if run_status in ["SUCCESS", "FAILURE", "CANCELED"]:
            print(f"\n{'✅' if run_status == 'SUCCESS' else '❌'} Run {run_status}")
            sys.exit(0 if run_status == "SUCCESS" else 1)

    print("\n⏱️ Timeout after 5 minutes")
else:
    error_type = result.get("data", {}).get("launchRun", {}).get("__typename", "Unknown")
    print(f"\n❌ Failed to launch: {error_type}")
    sys.exit(1)
