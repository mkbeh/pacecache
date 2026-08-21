#!/usr/bin/env bash
set -euo pipefail

root="${GITHUB_WORKSPACE:-$(pwd)}"
include_examples=false
include_benchmarks=false

for arg in "$@"; do
  case "${arg}" in
    --with-examples)
      include_examples=true
      ;;
    --with-benchmarks)
      include_benchmarks=true
      ;;
    *)
      echo "unknown argument: ${arg}" >&2
      exit 2
      ;;
  esac
done

cd "${root}"

rm -f go.work go.work.sum

modules=(.)

if [[ -f ./extra/paceotel/go.mod ]]; then
  modules+=(./extra/paceotel)
fi

if [[ "${include_examples}" == "true" ]]; then
  while IFS= read -r -d '' mod; do
    modules+=("$(dirname "${mod}")")
  done < <(find ./examples -name go.mod -type f -print0 2>/dev/null | sort -z)
fi

if [[ "${include_benchmarks}" == "true" ]]; then
  while IFS= read -r -d '' mod; do
    modules+=("$(dirname "${mod}")")
  done < <(find ./benchmarks -name go.mod -type f -print0 2>/dev/null | sort -z)
fi

# The workspace is ephemeral in CI and local-only for development.
GOWORK=off go work init "${modules[@]}"

# Local modules may already require release versions such as v0.1.0 while the
# corresponding tags do not exist yet. Bind those exact requirements to the
# current checkout so all Go tooling, including go/packages-based linters,
# resolves the unpublished modules locally.
while read -r module version; do
  [[ -n "${module}" && -n "${version}" ]] || continue

  case "${module}" in
    github.com/mkbeh/pacecache)
      local_dir="."
      ;;
    github.com/mkbeh/pacecache/*)
      local_dir="./${module#github.com/mkbeh/pacecache/}"
      ;;
    *)
      continue
      ;;
  esac

  if [[ -f "${root}/${local_dir#./}/go.mod" ]]; then
    GOWORK="${root}/go.work" go work edit \
      -replace="${module}@${version}=${local_dir}"
  fi
done < <(
  {
    printf '%s\0' ./go.mod

    find ./extra ./examples ./benchmarks \
      -name go.mod -type f -print0 2>/dev/null || true
  } |
  while IFS= read -r -d '' mod; do
    awk '
      $1 ~ /^github\.com\/mkbeh\/pacecache(\/.*)?$/ && $2 ~ /^v[0-9]/ {
        print $1, $2
      }
    ' "${mod}"
  done |
  sort -u
)

echo "Created temporary Go workspace at ${root}/go.work"

if [[ "${WORKSPACE_DEBUG:-false}" == "true" ]]; then
  GOWORK="${root}/go.work" go work edit -json
fi
