#!/usr/bin/env bash
# One-shot Vercel production deploy trigger for lng-trade-os-dmpc (www.lngtradeos.com).
# Use this after `git push` if Vercel auto-deploys aren't firing.
# Hook created 2026-04-25, name: main-prod-claude
HOOK_URL="https://api.vercel.com/v1/integrations/deploy/prj_syQJ6gOFncBnooCWxDRH96iTfDPU/nAU6XHNtqH"
echo "→ Triggering Vercel production deploy..."
RESP=$(curl -s -X POST "$HOOK_URL")
echo "$RESP"
JOB_ID=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('job',{}).get('id',''))" 2>/dev/null)
if [ -n "$JOB_ID" ]; then
  echo "→ Deploy job queued: $JOB_ID"
  echo "→ Live at https://www.lngtradeos.com in ~60-90 seconds"
else
  echo "⚠ Deploy hook returned unexpected response — check above"
fi
