#!/usr/bin/env python3
"""Trigger piezometry_chroniques_csv asset directly"""

import subprocess
import json

# Use dagster CLI to launch the job
cmd = [
    "docker", "exec", "brgm-dagster-webserver",
    "dagster", "job", "execute",
    "-m", "hubeau_pipeline",
    "--job", "__ASSET_JOB__",
    "--config-json", json.dumps({
        "ops": {
            "piezometry_chroniques_csv": {
                "config": {
                    "partition_key": "2024"
                }
            }
        },
        "assets": ["piezometry_chroniques_csv"]
    })
]

# Execute
result = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("Return code:", result.returncode)