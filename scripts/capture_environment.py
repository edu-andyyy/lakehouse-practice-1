"""Capture reproducibility metadata without publishing Docker credentials or host paths."""
import datetime
import json
from pathlib import Path
import platform
import subprocess

root = Path(__file__).resolve().parent.parent
output = root / "results"
output.mkdir(exist_ok=True)
info = json.loads((output / "docker-info.local.json").read_text())
ids = subprocess.check_output(["docker", "compose", "ps", "-aq"], cwd=root, text=True).split()
containers = json.loads(subprocess.check_output(["docker", "inspect", *ids], text=True))
image_ids = sorted({c["Image"] for c in containers})
images = json.loads(subprocess.check_output(["docker", "image", "inspect", *image_ids], text=True))
data = {
    "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "host_os": platform.platform(), "host_arch": platform.machine(),
    "python": platform.python_version(),
    "docker_server": info.get("ServerVersion"), "docker_cpu_count": info.get("NCPU"),
    "docker_memory_bytes": info.get("MemTotal"),
    "docker_arch": info.get("Architecture"),
    "images": [{"image_id": i["Id"], "repo_digests": i.get("RepoDigests", []),
                "architecture": i["Architecture"], "os": i["Os"]} for i in images],
    "containers": [{"service": c["Config"]["Labels"]["com.docker.compose.service"],
                    "image": c["Config"]["Image"], "image_id": c["Image"],
                    "memory_limit_bytes": c["HostConfig"]["Memory"],
                    "nano_cpus": c["HostConfig"].get("NanoCpus", 0),
                    "state": c["State"]["Status"]} for c in containers],
}
(output / "environment.json").write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
print("Параметры среды сохранены в results/environment.json")
