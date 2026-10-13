#!/usr/bin/env bash
set -euo pipefail

: "${OC_VM_HOST:?set OC_VM_HOST to the deployment SSH host}"
: "${OC_VM_USER:?set OC_VM_USER to the deployment SSH user}"
: "${OC_VM_SSH_KEY:?set OC_VM_SSH_KEY to the deployment private key}"
: "${OC_VM_KNOWN_HOSTS:?set OC_VM_KNOWN_HOSTS to the trusted SSH host keys}"
: "${DEPLOY_PATH:?set DEPLOY_PATH to the prepared environment directory}"
: "${ENVIRONMENT:?set ENVIRONMENT to staging or prod}"
: "${IMAGE:?set IMAGE to the service release tag}"
: "${EXPECTED_DIGEST:?set EXPECTED_DIGEST to the build output digest}"
: "${GHCR_USERNAME:?set GHCR_USERNAME to the GitHub actor}"
: "${GHCR_TOKEN:?set GHCR_TOKEN to this deployment job package-read token}"

[[ "$DEPLOY_PATH" = /* ]] || { printf '%s\n' 'DEPLOY_PATH must be absolute' >&2; exit 1; }
case "$ENVIRONMENT" in
  staging|prod) ;;
  *) printf '%s\n' 'ENVIRONMENT must be staging or prod' >&2; exit 1 ;;
esac
[[ "$IMAGE" =~ ^ghcr\.io/codetreker/remote-fs:[0-9]{6}-[1-9][0-9]*-[a-f0-9]{8}$ ]] || {
  printf '%s\n' 'IMAGE must be a ghcr.io/codetreker/remote-fs yymmdd-seq-hash8 tag' >&2
  exit 1
}
[[ "$EXPECTED_DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || { printf '%s\n' 'EXPECTED_DIGEST must be a SHA-256 digest' >&2; exit 1; }

repository_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
umask 077
mkdir -p -- "$repository_root/.tmp"
ssh_tmp=$(mktemp -d "$repository_root/.tmp/service-release.XXXXXX")
trap 'rm -rf -- "$ssh_tmp"' EXIT
printf '%s\n' "$OC_VM_SSH_KEY" > "$ssh_tmp/key"
printf '%s\n' "$OC_VM_KNOWN_HOSTS" > "$ssh_tmp/known_hosts"
ssh_options=(
  -F /dev/null
  -i "$ssh_tmp/key"
  -o "UserKnownHostsFile=$ssh_tmp/known_hosts"
  -o StrictHostKeyChecking=yes
  -o IdentitiesOnly=yes
  -o BatchMode=yes
)
ssh_target="$OC_VM_USER@$OC_VM_HOST"
scp "${ssh_options[@]}" -- "$repository_root/deployments/service/compose.yml" "$ssh_target:$DEPLOY_PATH/compose.yml"

{
  printf 'DEPLOY_PATH=%q\nIMAGE=%q\nEXPECTED_DIGEST=%q\nENVIRONMENT=%q\nGHCR_USERNAME=%q\nGHCR_TOKEN=%q\n' \
    "$DEPLOY_PATH" "$IMAGE" "$EXPECTED_DIGEST" "$ENVIRONMENT" "$GHCR_USERNAME" "$GHCR_TOKEN"
  cat <<'REMOTE'
set -euo pipefail
umask 077
cd -- "$DEPLOY_PATH"
[[ -f .env ]] || { printf '%s\n' 'DEPLOY_PATH must contain the environment .env file' >&2; exit 1; }
release_tmp=$(mktemp -d "$DEPLOY_PATH/.service-release.XXXXXX")
trap 'rm -rf -- "$release_tmp"' EXIT
export DOCKER_CONFIG="$release_tmp/docker"
mkdir -- "$DOCKER_CONFIG"
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username "$GHCR_USERNAME" --password-stdin
unset GHCR_TOKEN
docker pull "$IMAGE"
repo_digests=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$IMAGE")
grep -Fxq -- "ghcr.io/codetreker/remote-fs@$EXPECTED_DIGEST" <<< "$repo_digests" || {
  printf '%s\n' 'Pulled image does not match the build output digest' >&2
  exit 1
}
printf 'RFS_IMAGE=%s\n' "$IMAGE" > "$release_tmp/release.env"
export RFS_IMAGE="$IMAGE"
docker compose --project-name "remote-fs-$ENVIRONMENT" --file compose.yml \
  --env-file .env --env-file "$release_tmp/release.env" \
  up --pull never --no-build --wait --wait-timeout 180 app
mv -f -- "$release_tmp/release.env" release.env
REMOTE
} | ssh "${ssh_options[@]}" "$ssh_target" 'bash -se'
