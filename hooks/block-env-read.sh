Project hook — mini-chuck/.claude/hooks/block-env-read.sh
   #!/bin/bash
 
   INPUT=$(cat)
   TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name')
   TOOL_INPUT=$(echo "$INPUT" | jq -c '.tool_input')
 
   DENY='{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Blocked: reading .env files is not allowed. Ask the user for any env variable names you need."}}'
 
   case "$TOOL_NAME" in
     Read|Edit|Write)
       FILE_PATH=$(echo "$TOOL_INPUT" | jq -r '.file_path')
       if [[ "$(basename "$FILE_PATH")" == .env* ]]; then
         echo "$DENY"
         exit 0
       fi
       ;;
     Grep)
       SEARCH_PATH=$(echo "$TOOL_INPUT" | jq -r '.path // ""')
       GLOB_FILTER=$(echo "$TOOL_INPUT" | jq -r '.glob // ""')
       if [[ "$(basename "$SEARCH_PATH")" == .env* ]] || [[ "$GLOB_FILTER" == .env* ]] || [[ "$GLOB_FILTER" == *.env* ]]; then
         echo "$DENY"
         exit 0
       fi
       ;;
     Bash)
       COMMAND=$(echo "$TOOL_INPUT" | jq -r '.command')
       if echo "$COMMAND" | grep -qE '(cat|head|tail|less|more|bat|sed|awk|echo|printf|source|\.)\s+.*\.env'; then
         echo "$DENY"
         exit 0
       fi
       if echo "$COMMAND" | grep -qE '\.env'; then
         echo "$DENY"
         exit 0
       fi
       ;;
   esac
 
   exit 0
 
   The project hook is the stricter one — it also covers Grep and Bash commands that touch .env files.