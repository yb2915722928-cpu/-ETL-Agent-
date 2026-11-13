"""Build Aliyun runtime images with Docker directly (supports Chinese paths)."""
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
for target in ("postgres", "redis", "api", "web"):
    subprocess.run(
        [
            "docker", "build", "-f", "infra/aliyun/runtime.Dockerfile",
            "--target", target, "-t", f"etl-agent-aliyun-{target}:3.22.2", ".",
        ],
        cwd=root,
        check=True,
    )
