#!/usr/bin/env bash
# Run every test.py under this directory with python3 and print a pass/fail summary.
# Tests run one at a time because each one starts Docker and binds the same host port.
# A test passes when python3 exits 0. test.py exits 1 when the check fails.

set -u

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 was not found on PATH." >&2
    exit 127
fi

tests=()
while IFS= read -r test; do
    [ -n "$test" ] || continue
    tests+=("$test")
done < <(find "$root" -type f -name 'test.py' | LC_ALL=C sort)

if [[ ${#tests[@]} -eq 0 ]]; then
    echo "No test.py files found under $root" >&2
    exit 1
fi

echo "Found ${#tests[@]} test.py file(s). Running them one at a time."

passed=()
failed=()
index=0

for test in "${tests[@]}"; do
    index=$((index + 1))
    dir="$(dirname "$test")"
    if [[ "$dir" == "$root" ]]; then
        name="."
    else
        name="${dir#"$root"/}"
    fi

    echo ""
    echo "========== [$index/${#tests[@]}] $name =========="

    # Merge stderr into stdout so a later test header cannot appear in the middle of this log.
    (
        cd "$dir" || {
            echo "Cannot enter $dir" >&2
            exit 127
        }
        python3 -u test.py
    ) 2>&1 | cat
    code=${PIPESTATUS[0]}

    if [[ $code -eq 0 ]]; then
        passed+=("$name")
        echo "Result: PASSED"
    else
        failed+=("$name (exit $code)")
        echo "Result: FAILED (exit $code)"
    fi
done

echo ""
echo "========== Summary =========="
echo "Passed (${#passed[@]}):"
if [[ ${#passed[@]} -eq 0 ]]; then
    echo "  (none)"
else
    for item in "${passed[@]}"; do
        echo "  $item"
    done
fi

echo "Failed (${#failed[@]}):"
if [[ ${#failed[@]} -eq 0 ]]; then
    echo "  (none)"
else
    for item in "${failed[@]}"; do
        echo "  $item"
    done
fi

echo "Total: ${#tests[@]}  Passed: ${#passed[@]}  Failed: ${#failed[@]}"

if [[ ${#failed[@]} -gt 0 ]]; then
    exit 1
fi

exit 0
