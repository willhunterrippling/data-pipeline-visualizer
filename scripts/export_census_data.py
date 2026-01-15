#!/usr/bin/env python3
"""
Census Data Export Script

Fetches syncs and connections from the Census API and exports them to a JSON file
for use by the data-pipeline-visualizer indexer.

Usage:
    python export_census_data.py --api-key YOUR_API_KEY
    
    # Or using environment variable:
    export CENSUS_API_KEY=YOUR_API_KEY
    python export_census_data.py

    # Custom output path:
    python export_census_data.py --api-key YOUR_API_KEY --output ./my-census-data.json

Requirements:
    - Python 3.7+
    - requests library (pip install requests)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install it with: pip install requests")
    sys.exit(1)


CENSUS_API_BASE = "https://app.getcensus.com/api/v1"


def fetch_paginated(
    endpoint: str,
    api_key: str,
    params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """
    Fetch all pages from a Census API endpoint.
    
    Census API uses cursor-based pagination with 'next' links.
    """
    all_data: list[dict[str, Any]] = []
    url = f"{CENSUS_API_BASE}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    page = 1
    while url:
        print(f"  Fetching {endpoint} (page {page})...", end=" ", flush=True)
        
        response = requests.get(url, headers=headers, params=params if page == 1 else None)
        
        if response.status_code == 401:
            raise Exception("Authentication failed. Check your API key.")
        elif response.status_code == 403:
            raise Exception("Access forbidden. Your API key may not have permission for this endpoint.")
        elif response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
        
        data = response.json()
        
        # Census API returns data in a 'data' array
        items = data.get("data", [])
        all_data.extend(items)
        print(f"got {len(items)} items")
        
        # Check for next page
        # Census uses 'next' in the response for pagination
        next_url = data.get("next")
        if next_url:
            url = next_url
            page += 1
        else:
            url = None
    
    return all_data


def fetch_syncs(api_key: str) -> list[dict[str, Any]]:
    """Fetch all syncs from Census API."""
    print("\nFetching syncs...")
    return fetch_paginated("syncs", api_key)


def fetch_sources(api_key: str) -> list[dict[str, Any]]:
    """Fetch all source connections from Census API."""
    print("\nFetching sources (where data comes FROM)...")
    return fetch_paginated("sources", api_key)


def fetch_destinations(api_key: str) -> list[dict[str, Any]]:
    """Fetch all destination connections from Census API."""
    print("\nFetching destinations (where data goes TO)...")
    return fetch_paginated("destinations", api_key)


def export_census_data(api_key: str, output_path: str) -> None:
    """
    Export Census data to a JSON file.
    
    The output format matches the CensusConfig type expected by the indexer:
    {
        "syncs": [...],
        "connections": [...],  # Combined sources + destinations
        "_metadata": {
            "exportedAt": "...",
            "syncCount": N,
            "connectionCount": N
        }
    }
    """
    print("=" * 60)
    print("Census Data Export")
    print("=" * 60)
    
    # Fetch syncs
    syncs = fetch_syncs(api_key)
    
    # Fetch both sources and destinations, then merge into connections
    # Sources = where data comes FROM (e.g., your Snowflake warehouse)
    # Destinations = where data goes TO (e.g., Salesforce, or Snowflake for loop-backs)
    sources = fetch_sources(api_key)
    destinations = fetch_destinations(api_key)
    
    # Merge sources and destinations into a single connections array
    # The parser looks up connection types by ID for both source and destination
    connections = sources + destinations
    
    # Build output structure
    output = {
        "syncs": syncs,
        "connections": connections,
        "_metadata": {
            "exportedAt": datetime.now(timezone.utc).isoformat(),
            "syncCount": len(syncs),
            "sourceCount": len(sources),
            "destinationCount": len(destinations),
            "connectionCount": len(connections),
            "exportedBy": "export_census_data.py",
        }
    }
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"\nCreated directory: {output_dir}")
    
    # Write to file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    
    print("\n" + "=" * 60)
    print("Export Complete!")
    print("=" * 60)
    print(f"  Syncs:        {len(syncs)}")
    print(f"  Sources:      {len(sources)} (where data comes from)")
    print(f"  Destinations: {len(destinations)} (where data goes to)")
    print(f"  Output:       {os.path.abspath(output_path)}")
    print("\nNext steps:")
    print("  1. Review the exported file to ensure it looks correct")
    print("  2. Send the file to your teammate or commit it to the repo")
    print("  3. Place it at data/census.json in the data-pipeline-visualizer repo")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Census syncs and connections to a JSON file for the data-pipeline-visualizer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Using command line argument:
    python export_census_data.py --api-key YOUR_API_KEY

    # Using environment variable:
    export CENSUS_API_KEY=YOUR_API_KEY
    python export_census_data.py

    # Custom output path:
    python export_census_data.py --api-key YOUR_API_KEY --output ./census-export.json
        """
    )
    
    parser.add_argument(
        "--api-key",
        help="Census API key. Can also be set via CENSUS_API_KEY environment variable.",
        default=os.environ.get("CENSUS_API_KEY"),
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output file path (default: data/census.json)",
        default="data/census.json",
    )
    
    args = parser.parse_args()
    
    # Validate API key
    if not args.api_key:
        print("Error: Census API key is required.")
        print("Provide it via --api-key argument or CENSUS_API_KEY environment variable.")
        print("\nGet your API key from: https://app.getcensus.com/settings/api")
        sys.exit(1)
    
    try:
        export_census_data(args.api_key, args.output)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
