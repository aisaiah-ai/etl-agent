# Workato Recipe Backup & Restore Guide

| Field        | Value                                      |
|--------------|--------------------------------------------|
| **Author**   | IES Engineering                             |
| **Created**  | 2026-04-18                                  |
| **Status**   | Active                                      |
| **Location** | `scripts/workato_backup.py`                 |
| **Repo**     | `ies/etl-agent`                             |

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Generating a Workato API Token](#3-generating-a-workato-api-token)
4. [Backup Process](#4-backup-process)
   - 4.1 [List Available Projects](#41-list-available-projects)
   - 4.2 [Dry Run (Preview)](#42-dry-run-preview)
   - 4.3 [Export Recipes](#43-export-recipes)
   - 4.4 [Commit to GitHub](#44-commit-to-github)
5. [Restore Process](#5-restore-process)
   - 5.1 [Restore a Single Recipe](#51-restore-a-single-recipe)
   - 5.2 [Restore All Recipes](#52-restore-all-recipes)
   - 5.3 [Post-Restore Steps](#53-post-restore-steps)
6. [Output Structure](#6-output-structure)
7. [CLI Reference](#7-cli-reference)
8. [Script Source Code](#8-script-source-code)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Overview

This document describes how to back up Workato integration recipes to GitHub and restore them when needed. The process uses a Python script (`scripts/workato_backup.py`) that communicates with the Workato REST API to export recipe definitions as JSON files. These files can be committed to version control for audit trails, disaster recovery, and environment promotion.

**What gets backed up:**

- Recipe name, description, and metadata
- Recipe logic (triggers, actions, conditional steps)
- Folder/project associations
- Recipe status (running/stopped)

**What does NOT get backed up:**

- Connection credentials (OAuth tokens, passwords, API keys)
- Job history and logs
- Account-level settings

---

## 2. Prerequisites

| Requirement       | Details                                              |
|-------------------|------------------------------------------------------|
| Python            | 3.11 or later                                        |
| Workato API Token | Generated from Workato Settings (see Section 3)      |
| Network Access    | HTTPS access to `https://www.workato.com` (or your data center URL) |
| Git               | For committing backups to version control             |

No additional Python packages are required -- the script uses only the standard library.

---

## 3. Generating a Workato API Token

1. Log in to Workato at [https://app.workato.com](https://app.workato.com)
2. Navigate to **Settings** (gear icon in the left sidebar)
3. Select **API Keys** from the settings menu
4. Click **Create API Key**
5. Give the key a descriptive name (e.g., `recipe-backup-automation`)
6. Assign the following permissions:
   - **Recipes**: Read, Write (Write is needed for restore)
   - **Folders**: Read
7. Copy the generated token and store it securely

> **Security Note:** Treat this token like a password. Never commit it to source control. Use environment variables or a secrets manager.

---

## 4. Backup Process

### 4.1 List Available Projects

First, set your API token and discover which projects/folders exist in your Workato workspace:

```bash
export WORKATO_API_TOKEN="your-token-here"

python scripts/workato_backup.py --list-folders
```

**Expected output:**

```
Workato folders/projects:

  [12345] Workday Integration
  [12346] Test_Impot
  [12347] Salesforce Sync
```

Note the folder ID for the project you want to back up.

### 4.2 Dry Run (Preview)

Before exporting, preview which recipes will be included:

```bash
# By project name (partial match, case-insensitive)
python scripts/workato_backup.py --project "Workday Integration" --dry-run

# Or by folder ID
python scripts/workato_backup.py --folder-id 12345 --dry-run
```

**Expected output:**

```
Searching for project: Workday Integration
  Found: [12345] Workday Integration

Found 3 recipe(s).

  [1001] Sync employees from Workday  (running)
  [1002] Update user profiles         (running)
  [1003] Error notification handler    (stopped)
```

### 4.3 Export Recipes

Run the backup to download all recipes as JSON files:

```bash
# Export by project name
python scripts/workato_backup.py --project "Workday Integration"

# Export by folder ID
python scripts/workato_backup.py --folder-id 12345

# Export to a custom directory
python scripts/workato_backup.py --project "Workday Integration" \
  --output-dir workato/workday_integration

# Export ALL recipes across the workspace (no folder filter)
python scripts/workato_backup.py
```

**Expected output:**

```
Searching for project: Workday Integration
  Found: [12345] Workday Integration

Fetching recipe list (folder 12345)...
Found 3 recipe(s).

  [1/3] Exporting: Sync employees from Workday (id=1001)
  [2/3] Exporting: Update user profiles (id=1002)
  [3/3] Exporting: Error notification handler (id=1003)

Exported 3 recipes to workato/recipes/
Manifest: workato/recipes/_manifest.json
```

### 4.4 Commit to GitHub

After exporting, commit the backup to version control:

```bash
git add workato/
git commit -m "Backup Workato recipes: Workday Integration ($(date +%Y-%m-%d))"
git push
```

**Recommended schedule:** Run the backup weekly or before/after making significant recipe changes. This can be automated via GitHub Actions cron.

---

## 5. Restore Process

### 5.1 Restore a Single Recipe

To restore one recipe from a backup file:

```bash
export WORKATO_API_TOKEN="your-token-here"

# Preview first
python scripts/workato_backup.py --restore workato/recipes/sync_employees_from_workday_1001.json --dry-run

# Restore
python scripts/workato_backup.py --restore workato/recipes/sync_employees_from_workday_1001.json
```

**Expected output:**

```
  Restoring: Sync employees from Workday (original id=1001)...
    Created as new recipe id=2050 (STOPPED)
    Warning: Review connections and start manually in Workato UI
```

To restore into a specific folder:

```bash
python scripts/workato_backup.py --restore workato/recipes/sync_employees_from_workday_1001.json \
  --folder-id 12345
```

### 5.2 Restore All Recipes

To restore an entire backup directory at once:

```bash
# Preview
python scripts/workato_backup.py --restore-all workato/recipes/ --dry-run

# Restore all into the original folders
python scripts/workato_backup.py --restore-all workato/recipes/

# Restore all into a specific folder
python scripts/workato_backup.py --restore-all workato/recipes/ --folder-id 12345
```

**Expected output:**

```
Using manifest (3 recipes)

[1/3]  Restoring: Sync employees from Workday (original id=1001)...
    Created as new recipe id=2050 (STOPPED)
[2/3]  Restoring: Update user profiles (original id=1002)...
    Created as new recipe id=2051 (STOPPED)
[3/3]  Restoring: Error notification handler (original id=1003)...
    Created as new recipe id=2052 (STOPPED)

Done -- restored 3/3 recipes.
```

### 5.3 Post-Restore Steps

Restored recipes are created in **STOPPED** state. Before starting them, complete these steps:

| Step | Action | Details |
|------|--------|---------|
| 1    | **Verify connections** | Open each recipe in the Workato UI. Re-authenticate any connections (Workday, Salesforce, etc.) since credentials are not included in backups. |
| 2    | **Review triggers** | Confirm trigger settings (polling interval, webhook URLs, etc.) are correct for the target environment. |
| 3    | **Check folder placement** | Ensure recipes are in the correct project/folder. Move them if needed. |
| 4    | **Test with a dry run** | Use Workato's "Test recipe" feature to validate one execution before going live. |
| 5    | **Start recipes** | Toggle each recipe to "Running" once validated. |
| 6    | **Monitor job history** | Watch the first few executions in Workato's job history to confirm successful runs. |

> **Important:** Restore creates **new** recipes -- it does not overwrite or update existing ones. If the original recipe still exists, you may want to stop it before starting the restored copy to avoid duplicate processing.

---

## 6. Output Structure

After a backup, the output directory contains:

```
workato/recipes/
  _manifest.json                              # Index of all exported recipes
  sync_employees_from_workday_1001.json       # Full recipe definition
  update_user_profiles_1002.json              # Full recipe definition
  error_notification_handler_1003.json        # Full recipe definition
```

**Manifest format (`_manifest.json`):**

```json
{
  "exported_at": "2026-04-18T14:30:00+00:00",
  "recipe_count": 3,
  "folder_id": 12345,
  "recipes": [
    {
      "id": 1001,
      "name": "Sync employees from Workday",
      "file": "sync_employees_from_workday_1001.json",
      "running": true,
      "updated_at": "2026-04-15T10:22:00+00:00"
    }
  ]
}
```

**Recipe file contents:** Each JSON file contains the complete recipe definition as returned by the Workato API, including:
- `id` -- Original recipe ID
- `name` -- Recipe name
- `code` -- Recipe logic (trigger + actions as a JSON structure)
- `folder_id` -- Parent folder
- `running` -- Whether the recipe was active at time of export
- `created_at` / `updated_at` -- Timestamps

---

## 7. CLI Reference

```
usage: workato_backup.py [-h] [--folder-id ID] [--project NAME]
                         [--output-dir DIR] [--dry-run] [--list-folders]
                         [--restore FILE] [--restore-all DIR]
```

| Flag | Description |
|------|-------------|
| `--folder-id ID` | Workato folder/project ID to export or restore into |
| `--project NAME` | Search for a project by name (case-insensitive partial match) |
| `--output-dir DIR` | Output directory for exports (default: `workato/recipes`) |
| `--dry-run` | Preview actions without making changes |
| `--list-folders` | List all folders/projects in the workspace and exit |
| `--restore FILE` | Restore a single recipe from a JSON backup file |
| `--restore-all DIR` | Restore all recipes from a backup directory |

**Environment variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `WORKATO_API_TOKEN` | Yes | API token from Workato Settings |
| `WORKATO_BASE_URL` | No | API base URL (default: `https://www.workato.com`) |

---

## 8. Script Source Code

The backup/restore script is located at `scripts/workato_backup.py` in the repository. Below is the complete source for reference.

```python
"""Backup and restore Workato recipes via the Workato API.

Exports recipes as JSON files for version control, and can restore
them back to Workato from those same files.

Required env vars:
    WORKATO_API_TOKEN  -- API token from Workato Settings -> API Keys
    WORKATO_BASE_URL   -- (optional) defaults to https://www.workato.com

Usage -- Export:
    python scripts/workato_backup.py
    python scripts/workato_backup.py --folder-id 12345
    python scripts/workato_backup.py --project "Workday Integration"
    python scripts/workato_backup.py --output-dir workato/workday
    python scripts/workato_backup.py --dry-run

Usage -- Restore:
    python scripts/workato_backup.py --restore workato/recipes/my_recipe_12345.json
    python scripts/workato_backup.py --restore-all workato/recipes/
    python scripts/workato_backup.py --restore-all workato/recipes/ --folder-id 12345
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

        items = batch.get("items", batch) if isinstance(batch, dict) else batch
        if not items:
            break

        recipes.extend(items)

        if len(items) < per_page:
            break
        page += 1
        time.sleep(0.2)

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
            print(f"    SKIP -- failed to fetch: {e}", file=sys.stderr)
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

        time.sleep(0.2)

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
    """Restore a single recipe from a JSON backup file."""
    with open(filepath) as f:
        recipe_data = json.load(f)

    name = recipe_data.get("name", filepath.stem)
    original_id = recipe_data.get("id", "unknown")

    if dry_run:
        print(f"  [DRY RUN] Would restore: {name} (original id={original_id})")
        return recipe_data

    code = recipe_data.get("code")
    if isinstance(code, str):
        recipe_code = code
    elif isinstance(code, dict):
        recipe_code = json.dumps(code)
    else:
        print(f"  SKIP {name} -- no 'code' field found in backup", file=sys.stderr)
        return None

    payload = {
        "recipe": {
            "name": name,
            "code": recipe_code,
        }
    }

    if folder_id:
        payload["recipe"]["folder_id"] = folder_id
    elif recipe_data.get("folder_id"):
        payload["recipe"]["folder_id"] = recipe_data["folder_id"]

    for field in ("description",):
        if recipe_data.get(field):
            payload["recipe"][field] = recipe_data[field]

    print(f"  Restoring: {name} (original id={original_id})...")

    try:
        result = api_request("/recipes", base_url, token, method="POST", data=payload)
        new_id = result.get("id", "?")
        print(f"    Created as new recipe id={new_id} (STOPPED)")
        print(f"    Review connections and start manually in Workato UI")
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
    """Restore all recipe JSON files from a backup directory."""
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
        time.sleep(0.3)

    action = "would restore" if dry_run else "restored"
    print(f"\nDone -- {action} {len(results)}/{len(files)} recipes.")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Backup Workato recipes to JSON files for version control."
    )
    parser.add_argument("--folder-id", type=int, help="Workato folder/project ID")
    parser.add_argument("--project", type=str, help="Project name (partial match)")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    parser.add_argument("--list-folders", action="store_true", help="List folders and exit")
    parser.add_argument("--restore", type=str, metavar="FILE",
                        help="Restore a single recipe from a JSON file")
    parser.add_argument("--restore-all", type=str, metavar="DIR",
                        help="Restore all recipes from a backup directory")
    args = parser.parse_args()

    base_url = os.environ.get("WORKATO_BASE_URL", DEFAULT_BASE_URL)
    token = os.environ.get("WORKATO_API_TOKEN")

    if not token:
        print(
            "Error: WORKATO_API_TOKEN env var is required.\n"
            "Get your token from Workato -> Settings -> API Keys.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.list_folders:
        print("Workato folders/projects:\n")
        folders = list_folders(base_url, token)
        for f in folders:
            print(f"  [{f.get('id')}] {f.get('name')}")
        return

    if args.restore:
        fp = Path(args.restore)
        if not fp.exists():
            print(f"Error: file not found: {fp}", file=sys.stderr)
            sys.exit(1)
        restore_recipe(fp, base_url, token, args.folder_id, args.dry_run)
        return

    if args.restore_all:
        dp = Path(args.restore_all)
        if not dp.is_dir():
            print(f"Error: directory not found: {dp}", file=sys.stderr)
            sys.exit(1)
        restore_all_recipes(dp, base_url, token, args.folder_id, args.dry_run)
        return

    folder_id = args.folder_id
    if args.project and not folder_id:
        print(f"Searching for project: {args.project}")
        folder = find_folder_by_name(args.project, base_url, token)
        if folder:
            folder_id = folder["id"]
            print(f"  Found: [{folder_id}] {folder['name']}\n")
        else:
            print(f"  No folder matching '{args.project}' found.", file=sys.stderr)
            sys.exit(1)

    output_dir = Path(args.output_dir)
    export_recipes(base_url, token, folder_id, output_dir, args.dry_run)


if __name__ == "__main__":
    main()
```

---

## 9. Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| `API error 401` | Invalid or expired API token | Regenerate token in Workato Settings -> API Keys |
| `API error 403` | Token lacks required permissions | Ensure token has Read/Write access to Recipes and Read access to Folders |
| `API error 404` | Folder ID does not exist | Run `--list-folders` to verify the correct ID |
| `API error 429` | Rate limit exceeded | The script includes built-in delays; if still hitting limits, increase the `time.sleep()` values in the script |
| `Connection error` | Network issue or wrong base URL | Verify `WORKATO_BASE_URL` matches your data center (e.g., `https://app.eu.workato.com` for EU) |
| `No 'code' field found` | Backup file is corrupted or incomplete | Re-export the recipe from Workato |
| Restored recipe fails to run | Connections not re-authenticated | Open the recipe in Workato UI and reconnect all connections |
| Duplicate recipes after restore | Restore creates new recipes, does not update | Stop or delete the old recipe before starting the restored one |

**Data center URLs:**

| Region | Base URL |
|--------|----------|
| US     | `https://www.workato.com` (default) |
| EU     | `https://app.eu.workato.com` |
| JP     | `https://app.jp.workato.com` |
| AU     | `https://app.au.workato.com` |
| SG     | `https://app.sg.workato.com` |

---

*Last updated: 2026-04-18*
