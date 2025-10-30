import requests
import json

response = requests.post("http://localhost:8080/graphql", json={
    "query": """
    query {
      runOrError(runId: "3a213f8b-50fd-4f6d-9dac-996880a00653") {
        ... on Run {
          runId
          status
          stats {
            ... on RunStatsSnapshot {
              stepsSucceeded
              stepsFailed
              startTime
              endTime
            }
          }
        }
      }
    }
    """
})
print(json.dumps(response.json(), indent=2))
