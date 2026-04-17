name: Fetch ENTSOG Pipeline Data

on:
  schedule:
    - cron: '0 7,19 * * *'
  workflow_dispatch:

permissions:
  contents: write

jobs:
  fetch:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Fetch ENTSOG data
        run: python3 scripts/fetch_entsog.py

      - name: Commit and push
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/pipeline_daily.json
          git diff --cached --quiet || git commit -m "Update pipeline data $(date -u +%Y-%m-%d)"
          git push || true
