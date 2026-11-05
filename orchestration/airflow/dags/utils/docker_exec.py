"""Bounded Docker SDK execution with verified command completion."""
import docker


def docker_exec(container: str, cmd: list[str], timeout: int = 1200) -> str:
    client = docker.from_env(timeout=timeout)
    try:
        result = client.containers.get(container).exec_run(cmd=cmd)
        output = result.output.decode(errors="replace") if result.output else ""
        if result.exit_code != 0:
            raise RuntimeError(f"Container command failed (exit={result.exit_code}): {output[-4000:]}")
        return output
    finally:
        client.close()
