#!/usr/bin/env bash
set -euo pipefail
BR="bot/connector-smoke-$(date +%Y%m%d-%H%M%S)"
git checkout -b "$BR"
echo "Connector OK @ $(date -u) by ChatGPT helper" > CONNECTOR_OK.md
git add CONNECTOR_OK.md
git commit -m "chore: connector smoke test (by ChatGPT)"
git push -u origin "$BR"
echo
echo "✅ Pushed branch: $BR"
echo "Next: open a PR to main."
echo "If you have GitHub CLI: gh pr create -B main -H $BR -t 'Connector smoke' -b 'auto test'"
