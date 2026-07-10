"""
Deploy the 'Population Health & Care Quality Analytics' Genie space.
Creates the space if it doesn't exist, updates it if it does.
Sets permissions so all workspace users can interact with it.
"""
import sys
import json
import os


def main() -> None:
    if len(sys.argv) < 4:
        raise ValueError(
            "Usage: deploy_genie_space.py <catalog> <schema> <warehouse_id>"
        )
    catalog = sys.argv[1].strip()
    schema = sys.argv[2].strip()
    warehouse_id = sys.argv[3].strip()

    SPACE_TITLE = "Population Health & Care Quality Analytics"

    # --- Load the exported serialized_space.json ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config", "serialized_space.json")

    with open(config_path, "r") as f:
        exported = json.load(f)

    # The exported file has the full structure; extract the serialized_space
    if "serialized_space" in exported:
        space_config = exported["serialized_space"]
    else:
        space_config = exported

    # Ensure it's a dict (not already a string)
    if isinstance(space_config, str):
        space_config = json.loads(space_config)

    # --- Update table references to use the target catalog.schema ---
    if "data_sources" in space_config and "tables" in space_config["data_sources"]:
        for table in space_config["data_sources"]["tables"]:
            identifier = table.get("identifier", "")
            parts = identifier.split(".")
            if len(parts) == 3:
                table["identifier"] = f"{catalog}.{schema}.{parts[2]}"

    # Ensure version 2 is set (required by the API)
    space_config["version"] = 2

    # Serialize to JSON string (API requires a string, not a dict)
    serialized_space_str = json.dumps(space_config)

    # --- Connect to workspace ---
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    api = w.api_client

    # --- Find existing space by title ---
    existing_space_id = None
    page_token = None
    while True:
        params = {"page_size": "100"}
        if page_token:
            params["page_token"] = page_token
        resp = api.do("GET", "/api/2.0/genie/spaces", query=params)
        for space in resp.get("spaces", []):
            if space.get("title") == SPACE_TITLE:
                existing_space_id = space["space_id"]
                break
        if existing_space_id:
            break
        page_token = resp.get("next_page_token")
        if not page_token:
            break

    # --- Create or update ---
    description = exported.get("description", "")

    if existing_space_id:
        print(f"Updating existing Genie space: {existing_space_id}")
        api.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{existing_space_id}",
            body={
                "title": SPACE_TITLE,
                "description": description,
                "warehouse_id": warehouse_id,
                "serialized_space": serialized_space_str,
            },
        )
        space_id = existing_space_id
    else:
        print("Creating new Genie space")
        resp = api.do(
            "POST",
            "/api/2.0/genie/spaces",
            body={
                "title": SPACE_TITLE,
                "description": description,
                "warehouse_id": warehouse_id,
                "serialized_space": serialized_space_str,
            },
        )
        space_id = resp["space_id"]
        print(f"Created Genie space: {space_id}")

    # --- Set permissions: all workspace users can interact ---
    api.do(
        "PUT",
        f"/api/2.0/permissions/genie/{space_id}",
        body={
            "access_control_list": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_RUN",
                }
            ]
        },
    )
    print("Set permissions: CAN_RUN for all workspace users")

    # --- Print the URL ---
    host = w.config.host.rstrip("/")
    print(f"")
    print(f"Genie Space URL: {host}/genie/rooms/{space_id}")
    print(f"Title: {SPACE_TITLE}")
    print(f"Tables pointing to: {catalog}.{schema}.*")


if __name__ == "__main__":
    main()