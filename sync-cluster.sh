#!/bin/bash
################################################################################
# SCRIPT: sync-cluster.sh
# DESCRIPTION: Moves code from the laptop to a cluster master, and results back.
#              Run it on the laptop (Git Bash). Asks for the ssh password once
#              per call unless an ssh key is installed.
#
# USAGE:
#   bash sync-cluster.sh push [user@host]
#       Copies the files tracked by git (plus a VERSION file with the commit id)
#       into ~/my_scripts on the host; new files must be `git add`-ed first.
#       Existing files are overwritten; results/ and anything else already
#       there is left alone.
#   bash sync-cluster.sh pull [user@host] ['patterns']
#       Copies ~/my_scripts/results/<patterns> (default: 'pipeline_2*
#       storage_virtualization_loopback_* storage_bench_*') into the folder
#       that contains this repo, e.g. hadoop/pipeline_<timestamp>/.
#       Set REMOTE_RESULTS to pull from elsewhere (CloudLab wrapper runs write
#       to /scratch/results).
#
#   user@host defaults to mostufa.j@tapuz14.cslcs.technion.ac.il.
#   CloudLab example: bash sync-cluster.sh push Mostufa@er101.utah.cloudlab.us
################################################################################

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_HOST="mostufa.j@tapuz14.cslcs.technion.ac.il"

usage() {
    sed -n '/^# USAGE:/,/^####/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; /^###/d'
}

cmd=${1:-}
host=${2:-$DEFAULT_HOST}

case "$cmd" in
    push)
        cd "$REPO_DIR"
        # Scripts run on Linux: refuse Windows line endings ($'\r' breaks bash).
        crlf=$(git ls-files --eol | awk '$2 == "w/crlf" {print $NF}')
        if [[ -n "$crlf" ]]; then
            echo "These files have Windows (CRLF) line endings:"
            echo "$crlf" | sed 's/^/  /'
            echo "Convert them first, e.g.:  sed -i 's/\r\$//' <file>"
            exit 1
        fi
        version=$(git rev-parse --short HEAD)
        if ! git diff --quiet HEAD --; then
            version+="-dirty"
            echo "Note: uncommitted changes are included (VERSION: $version)."
        fi
        echo "$version $(date -Iseconds)" > VERSION
        trap 'rm -f "$REPO_DIR/VERSION"' EXIT
        echo "Pushing $version to $host:~/my_scripts ..."
        { git ls-files -z | while IFS= read -r -d '' f; do
              [[ -e "$f" ]] && printf '%s\0' "$f"
          done
          printf 'VERSION\0'
        } | tar --null -T - -czf - \
          | ssh "$host" 'mkdir -p ~/my_scripts && tar -xzf - -C ~/my_scripts && echo "  unpacked into $HOME/my_scripts"'
        echo "Done. (experiments/storage_virtualization_loopback_tapuz/ on the cluster, if still there, is obsolete.)"
        ;;
    pull)
        patterns=${3:-"pipeline_2* storage_virtualization_loopback_* storage_bench_*"}
        remote_results=${REMOTE_RESULTS:-'~/my_scripts/results'}
        dest="$(cd "$REPO_DIR/.." && pwd)"
        echo "Pulling $host:$remote_results/{$patterns} into $dest ..."
        # shellcheck disable=SC2086  # patterns are expanded on the cluster
        ssh "$host" "bash -s" -- "$remote_results" $patterns <<'REMOTE' | tar -xzf - -C "$dest"
dir=$1
shift
cd "${dir/#\~/$HOME}" || { echo "no results folder $dir" >&2; exit 1; }
shopt -s nullglob
files=()
for p in "$@"; do
    for f in $p; do files+=("$f"); done
done
if (( ${#files[@]} == 0 )); then
    echo "nothing on the cluster matches: $*" >&2
    exit 1
fi
tar -czf - --exclude=latest --exclude=pipeline_latest "${files[@]}"
REMOTE
        echo "Done."
        ;;
    *)
        usage
        exit 1
        ;;
esac
