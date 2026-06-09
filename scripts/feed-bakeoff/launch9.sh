#!/bin/bash
# 9-hour fully-detached launcher for the feed bake-off.
cd "$HOME/feed-bakeoff" || exit 1
exec env DUR=32400 OUT="$HOME/feed-bakeoff/run" python3 feed_bakeoff_v2.py \
    >"$HOME/feed-bakeoff/run.log" 2>&1 </dev/null
