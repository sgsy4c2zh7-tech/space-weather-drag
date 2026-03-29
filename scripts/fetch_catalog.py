name: Update Satellite Catalog and NOAA Data

on:
  workflow_dispatch:
  schedule:
    - cron: "10 0,4,8,12,16,20 * * *"

permissions:
  contents: write

jobs:
  update:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests

      - name: Prepare folders
        run: |
          mkdir -p docs/data
          mkdir -p docs/data/catalog
          mkdir -p docs/data/sat_history
          mkdir -p docs/data/noaa
          mkdir -p scripts

      - name: Fetch full CelesTrak catalog snapshot
        run: python scripts/fetch_catalog.py

      - name: Build 30-day satellite histories
        run: python scripts/build_sat_history.py

      - name: Fetch NOAA data
        run: python scripts/fetch_noaa.py

      - name: Commit and push
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add docs/data
          git commit -m "Update satellite catalog, histories, and NOAA data" || echo "No changes to commit"
          git push
