#!/usr/bin/env python3
"""Script to manually trigger piezometry_chroniques_csv asset for testing"""

import requests
import json
import time

# GraphQL endpoint
url = "http://localhost:8080/graphql"

# First, get repository info
query_repos = """
query {
  repositoryLocations {
    name
    repositories {
      name
      pipelines {
        name
      }
    }
  }
}
"""

response = requests.post(url, json={"query": query_repos})
print("Repository info:", json.dumps(response.json(), indent=2))

# Try to launch the asset job
query_launch = """
mutation {
  launchRun(
    executionParams: {
      selector: {
        repositoryLocationName: "hubeau_pipeline"
        repositoryName: "__repository__"
        pipelineName: "__ASSET_JOB__"
        assetSelection: [
          {
            path: ["piezometry_chroniques_csv"]
          }
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
      }
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

response = requests.post(url, json={"query": query_launch})
print("\nLaunch response:", json.dumps(response.json(), indent=2))

# If successful, get the run ID
result = response.json()
if result.get("data", {}).get("launchRun", {}).get("__typename") == "LaunchRunSuccess":
    run_id = result["data"]["launchRun"]["run"]["runId"]
    print(f"\n✅ Run launched successfully! Run ID: {run_id}")
    print(f"View at: http://localhost:8080/runs/{run_id}")
else:
    print("\n❌ Failed to launch run")