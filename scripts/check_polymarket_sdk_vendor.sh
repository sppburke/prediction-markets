#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

download_and_extract() {
    local crate_name=$1
    local version=$2
    local expected_sha=$3
    local destination=$4
    local archive="$work_dir/${crate_name}.crate"
    curl -fsSL "https://static.crates.io/crates/${crate_name}/${crate_name}-${version}.crate" \
        -o "$archive"
    printf '%s  %s\n' "$expected_sha" "$archive" | sha256sum --check --status
    mkdir "$destination"
    tar -xzf "$archive" -C "$destination" --strip-components=1
}

collect_delta() {
    local upstream=$1
    local vendor=$2
    local line local_path directory file
    while IFS= read -r line; do
        if [[ $line == Files*" differ" ]]; then
            local_path=${line#* and }
            local_path=${local_path% differ}
            printf 'changed %s\n' "${local_path#"$vendor"/}"
        elif [[ $line == "Only in $vendor"* ]]; then
            directory=${line%%: *}
            directory=${directory#"Only in $vendor"}
            directory=${directory#/}
            file=${line#*: }
            printf 'added %s%s%s\n' "$directory" "${directory:+/}" "$file"
        elif [[ $line == "Only in $upstream"* ]]; then
            directory=${line%%: *}
            directory=${directory#"Only in $upstream"}
            directory=${directory#/}
            file=${line#*: }
            printf 'removed %s%s%s\n' "$directory" "${directory:+/}" "$file"
        else
            printf 'unrecognized diff: %s\n' "$line" >&2
            return 1
        fi
    done < <(diff -qr --exclude target "$upstream" "$vendor" || true)
}

sdk_upstream="$work_dir/sdk-upstream"
download_and_extract polymarket_client_sdk_v2 0.7.0 \
    ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a \
    "$sdk_upstream"

expected_sdk_delta=$'added PROVENANCE.md\nchanged Cargo.lock\nchanged Cargo.toml\nchanged Cargo.toml.orig\nchanged src/auth.rs\nchanged src/clob/client.rs\nchanged src/clob/mod.rs\nchanged src/lib.rs'
actual_sdk_delta=$(collect_delta \
    "$sdk_upstream" "$repo_root/third_party/polymarket_client_sdk_v2" | LC_ALL=C sort)
[[ $actual_sdk_delta == "$expected_sdk_delta" ]] || {
    printf 'unexpected Polymarket SDK vendor delta:\n%s\n' "$actual_sdk_delta" >&2
    exit 1
}

effective_tree_hash=$(
    cd "$repo_root"
    find third_party -type f -not -path '*/target/*' -print0 \
        | LC_ALL=C sort -z \
        | xargs -0 sha256sum \
        | sha256sum \
        | cut -d' ' -f1
)
embedded_tree_hash=$(awk '
    /SDK_EFFECTIVE_VENDOR_TREE_SHA256/ { getline; gsub(/[";]/, ""); gsub(/^[[:space:]]+/, ""); print; exit }
' "$repo_root/crates/service/src/bin/pe-service-live-canary.rs")
[[ $effective_tree_hash == "$embedded_tree_hash" ]] || {
    printf 'effective vendor tree hash mismatch: calculated %s, embedded %s\n' \
        "$effective_tree_hash" "$embedded_tree_hash" >&2
    exit 1
}

printf 'OK: Polymarket SDK archives, allowlisted deltas, and effective tree hash verified\n'
