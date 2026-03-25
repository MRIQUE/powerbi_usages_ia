from __future__ import annotations
import time
import requests


class FabricLROError(Exception):
    pass


def poll_lro(
    location_url: str,
    headers: dict,
    timeout_s: int = 300,
    poll_interval: int = 30,
) -> dict:
    """Poll a Fabric Long Running Operation until completion.

    Phase 1: GET location_url until status is Succeeded or Failed.
    Phase 2: GET location_url/result to retrieve the created resource.

    Returns the resource dict from phase 2.
    Raises FabricLROError on failure, TimeoutError on timeout.
    """
    start = time.monotonic()

    while True:
        if time.monotonic() - start > timeout_s:
            raise TimeoutError(
                f"LRO timed out after {timeout_s}s: {location_url}"
            )

        resp = requests.get(location_url, headers=headers)
        resp.raise_for_status()
        body = resp.json()
        status = body.get("status", "Unknown")

        if status == "Succeeded":
            break
        if status == "Failed":
            error_msg = body.get("error", {}).get("message", "Unknown error")
            raise FabricLROError(f"LRO failed: {error_msg}")

        # Still running — respect Retry-After if present, else use poll_interval
        delay = int(resp.headers.get("Retry-After", poll_interval))
        time.sleep(delay)

    # Phase 2: retrieve the created resource
    result_resp = requests.get(f"{location_url}/result", headers=headers)
    result_resp.raise_for_status()
    return result_resp.json()
