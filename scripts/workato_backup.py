"""Backup and restore Workato recipes via the Workato API.

Exports recipes as JSON files for version control, and can restore
them back to Workato from those same files.

Required env vars:
    WORKATO_API_TOKEN  — API token from Workato Settings → API Keys
    WORKATO_BASE_URL   — (optional) defaults to https://www.workato.com

Usage — Export:
    # Export all recipes
    python scripts/workato_backup.py

    # Export a specific folder/project by ID
    python scripts/workato_backup.py --folder-id 12345

    # Export by project name (searches for matching folder)
    python scripts/workato_backup.py --project "Workday Integration"

    # Custom output directory
    python scripts/workato_backup.py --output-dir workato/workday

    # Dry run — list recipes without saving
    python scripts/workato_backup.py --dry-run

Usage — Restore:
    # Restore a single recipe from a backup file
    python scripts/workato_backup.py --restore workato/recipes/my_recipe_12345.json

    # Restore all recipes from a backup directory
    python scripts/workato_backup.py --restore-all workato/recipes/

    # Restore into a specific folder
    python scripts/workato_backup.py --restore-all workato/recipes/ --folder-id 12345

    # Dry run — show what would be restored without making changes
    python scripts/workato_backup.py --restore-all workato/recipes/ --dry-run
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://www.workato.com"
DEFAULT_OUTPUT_DIR = "workato/recipes"


def api_request(
    path: str,
    base_url: str,
    token: str,
    method: str = "GET",
    data: dict | None = None,
) -> dict | list:
    """Make an authenticated request to the Workato API."""
    url = f"{base_url.rstrip('/')}/api{path}"
    body = json.dumps(data).encode() if data else None
    req = Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        err_body = e.read().decode() if e.fp else ""
        print(f"  API error {e.code} for {url}: {err_body}", file=sys.stderr)
        raise
    except URLError as e:
        print(f"  Connection error for {url}: {e.reason}", file=sys.stderr)
        raise


def list_folders(base_url: str, token: str, parent_id: int | None = None) -> list[dict]:
    """List folders/projects, optionally under a parent."""
    path = "/folders"
    if parent_id:
        path += f"?parent_id={parent_id}"
    return api_request(path, base_url, token)


def find_folder_by_name(name: str, base_url: str, token: str) -> dict | None:
    """Search for a folder/project by name (case-insensitive partial match)."""
    folders = list_folders(base_url, token)
    name_lower = name.lower()
    for folder in folders:
        if name_lower in folder.get("name", "").lower():
            return folder
    return None


def list_recipes(
    base_url: str, token: str, folder_id: int | None = None
) -> list[dict]:
    """List all recipes, optionally filtered by folder. Handles pagination."""
    recipes = []
    page = 1
    per_page = 100

    while True:
        path = f"/recipes?per_page={per_page}&page={page}"
        if folder_id:
            path += f"&folder_id={folder_id}"

        batch = api_request(path, base_url, token)
        if not batch:
            break

        # API returns {"items": [...]} or a bare list depending on version
        items = batch.get("items", batch) if isinstance(batch, dict) else batch
        if not items:
            break

        recipes.extend(items)

        if len(items) < per_page:
            break
        page += 1
        time.sleep(0.2)  # rate-limit courtesy

    return recipes


def get_recipe(recipe_id: int, base_url: str, token: str) -> dict:
    """Get full recipe details by ID."""
    return api_request(f"/recipes/{recipe_id}", base_url, token)


def sanitize_filename(name: str) -> str:
    """Convert a recipe name to a safe filename."""
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name.lower()[:80]


def export_recipes(
    base_url: str,
    token: str,
    folder_id: int | None,
    output_dir: Path,
    dry_run: bool = False,
) -> list[dict]:
    """Export all recipes from Workato and save as JSON files."""
    print(f"Fetching recipe list{f' (folder {folder_id})' if folder_id else ''}...")
    recipes = list_recipes(base_url, token, folder_id)

    if not recipes:
        print("No recipes found.")
        return []

    print(f"Found {len(recipes)} recipe(s).\n")

    if dry_run:
        for r in recipes:
            status = r.get("running", False)
            print(
                f"  [{r.get('id')}] {r.get('name')}"
                f"  ({'running' if status else 'stopped'})"
            )
        return recipes

    output_dir.mkdir(parents=True, exist_ok=True)

    # Save manifest
    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "recipe_count": len(recipes),
        "folder_id": folder_id,
        "recipes": [],
    }

    for i, recipe_summary in enumerate(recipes, 1):
        rid = recipe_summary["id"]
        name = recipe_summary.get("name", f"recipe_{rid}")
        print(f"  [{i}/{len(recipes)}] Exporting: {name} (id={rid})")

        try:
            recipe = get_recipe(rid, base_url, token)
        except Exception as e:
            print(f"    SKIP — failed to fetch: {e}", file=sys.stderr)
            continue

        filename = f"{sanitize_filename(name)}_{rid}.json"
        filepath = output_dir / filename

        with open(filepath, "w") as f:
            json.dump(recipe, f, indent=2, default=str)

        manifest["recipes"].append(
            {
                "id": rid,
                "name": name,
                "file": filename,
                "running": recipe.get("running", False),
                "updated_at": recipe.get("updated_at", ""),
            }
        )

        time.sleep(0.2)  # rate-limit courtesy

    # Write manifest
    manifest_path = output_dir / "_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    print(f"\nExported {len(manifest['recipes'])} recipes to {output_dir}/")
    print(f"Manifest: {manifest_path}")
    return recipes


def restore_recipe(
    filepath: Path,
    base_url: str,
    token: str,
    folder_id: int | None = None,
    dry_run: bool = False,
) -> dict | None:
    """Restore a single recipe from a JSON backup file.

    Creates a new recipe in Workato from the exported JSON. The recipe
    is created in stopped state — you must manually start it after
    verifying connections are correct.
    """
    with open(filepath) as f:
        recipe_data = json.load(f)

    name = recipe_data.get("name", filepath.stem)
    original_id = recipe_data.get("id", "unknown")

    if dry_run:
        print(f"  [DRY RUN] Would restore: {name} (original id={original_id})")
        return recipe_data

    # Build the payload for recipe creation.
    # The Workato API expects a "recipe" wrapper with name, code, and folder_id.
    # "code" is the recipe logic (trigger + actions) stored as a JSON string.
    code = recipe_data.get("code")
    if isinstance(code, str):
        # Already a JSON string
        recipe_code = code
    elif isinstance(code, dict):
        recipe_code = json.dumps(code)
    else:
        print(f"  SKIP {name} — no 'code' field found in backup", file=sys.stderr)
        return None

    payload = {
        "recipe": {
            "name": name,
            "code": recipe_code,
        }
    }

    # Optional: place in a specific folder
    if folder_id:
        payload["recipe"]["folder_id"] = folder_id
    elif recipe_data.get("folder_id"):
        payload["recipe"]["folder_id"] = recipe_data["folder_id"]

    # Copy over optional fields if present
    for field in ("description",):
        if recipe_data.get(field):
            payload["recipe"][field] = recipe_data[field]

    print(f"  Restoring: {name} (original id={original_id})...")

    try:
        result = api_request("/recipes", base_url, token, method="POST", data=payload)
        new_id = result.get("id", "?")
        print(f"    Created as new recipe id={new_id} (STOPPED)")
        print(f"    ⚠ Review connections and start manually in Workato UI")
        return result
    except Exception as e:
        print(f"    FAILED: {e}", file=sys.stderr)
        return None


def restore_all_recipes(
    directory: Path,
    base_url: str,
    token: str,
    folder_id: int | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Restore all recipe JSON files from a backup directory.

    Reads the _manifest.json if present to determine order,
    otherwise restores all .json files alphabetically.
    """
    manifest_path = directory / "_manifest.json"
    files: list[Path] = []

    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)
        print(f"Using manifest ({len(manifest.get('recipes', []))} recipes)\n")
        for entry in manifest.get("recipes", []):
            fp = directory / entry["file"]
            if fp.exists():
                files.append(fp)
            else:
                print(f"  WARN: {entry['file']} listed in manifest but not found")
    else:
        files = sorted(
            p for p in directory.glob("*.json") if p.name != "_manifest.json"
        )
        print(f"No manifest found. Found {len(files)} JSON file(s) in {directory}/\n")

    if not files:
        print("No recipe files to restore.")
        return []

    results = []
    for i, fp in enumerate(files, 1):
        print(f"[{i}/{len(files)}]", end="")
        result = restore_recipe(fp, base_url, token, folder_id, dry_run)
        if result:
            results.append(result)
        time.sleep(0.3)  # rate-limit courtesy

    action = "would restore" if dry_run else "restored"
    print(f"\nDone — {action} {len(results)}/{len(files)} recipes.")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Backup Workato recipes to JSON files for version control."
    )
    parser.add_argument(
        "--folder-id",
        type=int,
        help="Workato folder/project ID to export",
    )
    parser.add_argument(
        "--project",
        type=str,
        help="Project name to search for (partial match)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List recipes without exporting",
    )
    parser.add_argument(
        "--list-folders",
        action="store_true",
        help="List all folders/projects and exit",
    )
    parser.add_argument(
        "--restore",
        type=str,
        metavar="FILE",
        help="Restore a single recipe from a JSON backup file",
    )
    parser.add_argument(
        "--restore-all",
        type=str,
        metavar="DIR",
        help="Restore all recipes from a backup directory",
    )
    args = parser.parse_args()

    base_url = os.environ.get("WORKATO_BASE_URL", DEFAULT_BASE_URL)
    token = os.environ.get("WORKATO_API_TOKEN")

    if not token:
        print(
            "Error: WORKATO_API_TOKEN env var is required.\n"
            "Get your token from Workato → Settings → API Keys.",
            file=sys.stderr,
        )
        sys.exit(1)

    # List folders mode
    if args.list_folders:
        print("Workato folders/projects:\n")
        folders = list_folders(base_url, token)
        for f in folders:
            print(f"  [{f.get('id')}] {f.get('name')}")
        return

    # Restore mode — single file
    if args.restore:
        fp = Path(args.restore)
        if not fp.exists():
            print(f"Error: file not found: {fp}", file=sys.stderr)
            sys.exit(1)
        restore_recipe(fp, base_url, token, args.folder_id, args.dry_run)
        return

    # Restore mode — entire directory
    if args.restore_all:
        dp = Path(args.restore_all)
        if not dp.is_dir():
            print(f"Error: directory not found: {dp}", file=sys.stderr)
            sys.exit(1)
        restore_all_recipes(dp, base_url, token, args.folder_id, args.dry_run)
        return

    # Resolve project name to folder ID
    folder_id = args.folder_id
    if args.project and not folder_id:
        print(f"Searching for project: {args.project}")
        folder = find_folder_by_name(args.project, base_url, token)
        if folder:
            folder_id = folder["id"]
            print(f"  Found: [{folder_id}] {folder['name']}\n")
        else:
            print(f"  No folder matching '{args.project}' found.", file=sys.stderr)
            print("  Use --list-folders to see available projects.", file=sys.stderr)
            sys.exit(1)

    output_dir = Path(args.output_dir)
    export_recipes(base_url, token, folder_id, output_dir, args.dry_run)


if __name__ == "__main__":
    main()
