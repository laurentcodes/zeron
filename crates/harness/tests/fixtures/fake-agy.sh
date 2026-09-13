#!/bin/sh
# fake antigravity cli for zeron-harness tests, driven by
# crates/harness/tests/antigravity.rs. mirrors the stream-json wire captured
# live from agy 1.2.2; the scenario is picked from each prompt's text.

emit() { printf '%s\n' "$1"; }
has() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac; }

args="$*"

if [ "$1" = "models" ]; then
  echo "Fetching available models..."
  printf 'gemini-3.8-flash-high\tGemini 3.8 Flash (High)\n'
  printf 'gemini-3.8-flash-low\tGemini 3.8 Flash (Low)\n'
  printf 'claude-opus-4-6-thinking\tClaude Opus 4.6 (Thinking)\n'
  exit 0
fi

if has "$args" "-p=/skills"; then
  has "$args" "--output-format json" || { echo "/skills listing needs --output-format json" >&2; exit 2; }
  emit '{"conversation_id":"","status":"SUCCESS","response":"","command":{"name":"skills","data":{"skills":[{"name":"agy-customizations","description":"Comprehensive guide to the Antigravity customization system. Use to explain how customizations work.","path":"/b/agy-customizations/SKILL.md","builtin":true,"model_invocable":true},{"name":"animate","description":"Design and build web animations that feel right","path":"/g/animate/SKILL.md","builtin":false,"model_invocable":true},{"name":"animate","description":"duplicate from a lower-priority root","path":"/h/animate/SKILL.md","builtin":false,"model_invocable":true},{"name":" ","description":"blank names are skipped","path":"/x/SKILL.md","builtin":false,"model_invocable":true}]}}}'
  exit 0
fi

model=""
prev=""
for arg in "$@"; do
  [ "$prev" = "--model" ] && model="$arg"
  prev="$arg"
done

has "$args" "--input-format stream-json" || { echo "missing --input-format stream-json" >&2; exit 2; }
has "$args" "--output-format stream-json" || { echo "missing --output-format stream-json" >&2; exit 2; }
has "$args" "--add-dir " || { echo "missing --add-dir" >&2; exit 2; }
has "$args" "--dangerously-skip-permissions" || { echo "missing --dangerously-skip-permissions" >&2; exit 2; }
case "$args" in
  *" -p=") ;;
  *) echo "-p= must be the last argument" >&2; exit 2 ;;
esac

conv="conv-fake"
has "$args" "--conversation conv-resume" && conv="conv-resume"

emit "{\"event\":\"init\",\"conversation_id\":\"$conv\",\"init\":{\"cwd\":\"/w\",\"tools\":[\"run_command\",\"write_to_file\"],\"permission_mode\":\"request-review\"}}"

step=0
turn=0

step_update() { # $1 = body after step_index
  emit "{\"event\":\"step_update\",\"step_update\":{\"conversation_id\":\"$conv\",\"step_index\":$step,$1}}"
}

tool_step() { # $1 = state, $2 = extra tool_info fields
  step_update "\"state\":\"$1\",\"step_type\":\"tool\",\"tool_name\":\"run_command\",\"tool_info\":{\"name\":\"run_command\",\"parameters\":{\"CommandLine\":\"ls -la\"}$2}"
}

while read -r line; do
  has "$line" '"event":"user"' || continue
  turn=$((turn + 1))
  denied="[]"
  reply="reply $turn"
  has "$line" "which model" && reply="model $model"

  step_update "\"state\":\"DONE\",\"step_type\":\"user_input\""
  step=$((step + 1))

  if has "$line" "crash"; then
    echo "boom: fake agy crashed" >&2
    exit 3
  fi
  if has "$line" "hang"; then
    exec sleep 30
  fi

  if has "$line" "use a tool"; then
    tool_step ACTIVE ""
    tool_step DONE ""
    step=$((step + 1))
  fi

  if has "$line" "deny"; then
    tool_step ACTIVE ""
    tool_step ERROR ",\"error\":{\"type\":\"TOOL_ERROR\",\"message\":\"permission check failed for command ls -la\"}"
    step=$((step + 1))
    denied='[{"action":"command","display_name":"RunCommand"}]'
  fi

  step_update "\"state\":\"ACTIVE\",\"step_type\":\"agent_response\",\"text_delta\":\"$reply\""
  step_update "\"state\":\"DONE\",\"step_type\":\"agent_response\",\"usage\":{\"input_tokens\":1000,\"output_tokens\":5}"
  step=$((step + 1))

  emit "{\"event\":\"result\",\"result\":{\"conversation_id\":\"$conv\",\"status\":\"SUCCESS\",\"response\":\"$reply\",\"num_turns\":$turn,\"denied_actions\":$denied}}"
done
