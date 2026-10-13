#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p -- "$repository_root/.tmp"
test_tmp=$(mktemp -d "$repository_root/.tmp/test-deploy-service.XXXXXX")
trap 'rm -rf -- "$test_tmp"' EXIT
mkdir -- "$test_tmp/bin"

cat > "$test_tmp/bin/mock" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
command_name=${0##*/}
if [[ "$command_name" = ssh || "$command_name" = scp ]]; then
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -F|-i|-o)
        if [[ "$1" = -i ]]; then
          [[ -f "$2" && $(stat -c '%a' "$2") = 600 ]]
          printf '%s\n' "${2%/key}" >> "$MOCK_CASE/ssh-directories"
        elif [[ "$1" = -o && "$2" = UserKnownHostsFile=* ]]; then
          [[ -f "${2#*=}" && $(stat -c '%a' "${2#*=}") = 600 ]]
        fi
        shift 2 ;;
      --) shift; break ;;
      *) break ;;
    esac
  done
  printf '%s\n' "$command_name" >> "$MOCK_CASE/transport-events"
  [[ "$MOCK_FAIL" != "$command_name" ]] || exit 45
  if [[ "$command_name" = scp ]]; then
    [[ $# = 2 && "$2" = 'operator@vm.invalid:'* ]]
    cp -- "$1" "${2#operator@vm.invalid:}"
  else
    [[ $# = 2 && "$1" = operator@vm.invalid && "$2" = 'bash -se' ]]
    bash -se
  fi
  exit
fi

[[ "$command_name" = docker ]]
[[ "$DOCKER_CONFIG" = "$MOCK_DEPLOY"/.service-release.*/docker ]]
[[ $(stat -c '%a' "$DOCKER_CONFIG") = 700 ]]
printf '%s\n' "$DOCKER_CONFIG" >> "$MOCK_CASE/docker-directories"
case "$1" in
  login)
    [[ "$*" = 'login ghcr.io --username test-actor --password-stdin' ]]
    token=$(cat)
    [[ "$token" = test-package-token ]]
    printf '%s\n' login >> "$MOCK_CASE/docker-events"
    printf '%s\n' '{"auths":{"ghcr.io":{"auth":"fake"}}}' > "$DOCKER_CONFIG/config.json"
    [[ "$MOCK_FAIL" != login ]] || exit 42
    ;;
  pull)
    [[ $# = 2 && "$2" = "$MOCK_IMAGE" && -f "$DOCKER_CONFIG/config.json" ]]
    printf '%s\n' pull >> "$MOCK_CASE/docker-events"
    [[ "$MOCK_FAIL" != pull ]] || exit 43
    ;;
  image)
    [[ $# = 5 && "$2" = inspect && "$3" = --format && "$5" = "$MOCK_IMAGE" ]]
    [[ "$4" = '{{range .RepoDigests}}{{println .}}{{end}}' ]]
    printf '%s\n' inspect >> "$MOCK_CASE/docker-events"
    [[ "$MOCK_FAIL" != inspect ]] || exit 46
    case "$MOCK_FAIL" in
      mismatch) printf 'ghcr.io/codetreker/remote-fs@%s-extra\n' "$MOCK_DIGEST" ;;
      image-id) printf '%s\n' "$MOCK_DIGEST" ;;
      *) printf 'ghcr.io/another/repository@%s\nghcr.io/codetreker/remote-fs@%s\n' "$MOCK_DIGEST" "$MOCK_DIGEST" ;;
    esac
    ;;
  compose)
    [[ -f "$DOCKER_CONFIG/config.json" ]]
    [[ "$PWD" = "$MOCK_DEPLOY" && -f compose.yml && -f .env ]]
    project=
    candidate=
    shift
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --project-name) project=$2; shift 2 ;;
        --file) [[ "$2" = compose.yml ]]; shift 2 ;;
        --env-file)
          [[ -f "$2" ]]
          [[ "$2" = .env ]] || candidate=$2
          shift 2 ;;
        up) shift; break ;;
        *) exit 90 ;;
      esac
    done
    [[ "$*" = '--pull never --no-build --wait --wait-timeout 180 app' ]]
    [[ $(cat -- "$candidate") = "RFS_IMAGE=$RFS_IMAGE" ]]
    [[ "$RFS_IMAGE" = "$MOCK_IMAGE" ]]
    printf '%s\n' "$project" > "$MOCK_CASE/project"
    printf '%s\n' "$RFS_IMAGE" > "$MOCK_CASE/image"
    printf '%s\n' wait >> "$MOCK_CASE/docker-events"
    [[ "$MOCK_FAIL" != wait ]] || exit 44
    ;;
  *) exit 91 ;;
esac
MOCK
chmod +x "$test_tmp/bin/mock"
for command_name in ssh scp docker; do
  ln -s mock "$test_tmp/bin/$command_name"
done

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_equal() {
  [[ "$1" = "$2" ]] || fail "$3"
}

image=ghcr.io/codetreker/remote-fs:261013-2-0123abcd
previous_image=ghcr.io/codetreker/remote-fs:261013-1-deadbeef
expected_digest=sha256:$(printf '%064d' 1)
case_number=0

run_case() {
  local environment=$1 injected_failure=$2 expected_status=$3 name=$4
  local deployment case_dir status directory
  case_number=$((case_number + 1))
  case_dir="$test_tmp/case-$case_number"
  deployment="$case_dir/$name"
  mkdir -p -- "$deployment/state" "$case_dir/prepared"
  printf 'RFS_HTTP_PORT=18080\n' > "$deployment/.env"
  printf 'prepared service config\n' > "$deployment/config.yml"
  printf 'persistent users and volumes\n' > "$deployment/state/catalog"
  printf 'RFS_IMAGE=%s\n' "$previous_image" > "$deployment/release.env"
  cp -- "$deployment/.env" "$deployment/config.yml" "$deployment/state/catalog" "$case_dir/prepared/"
  : > "$case_dir/docker-events"
  : > "$case_dir/transport-events"
  status=0
  (
    cd -- "$test_tmp"
    PATH="$test_tmp/bin:$PATH" MOCK_CASE="$case_dir" MOCK_DEPLOY="$deployment" \
      MOCK_FAIL="$injected_failure" MOCK_IMAGE="$image" MOCK_DIGEST="$expected_digest" \
      OC_VM_HOST=vm.invalid OC_VM_USER=operator OC_VM_SSH_KEY=test-ssh-private-key \
      OC_VM_KNOWN_HOSTS='vm.invalid ssh-ed25519 test-host-key' \
      DEPLOY_PATH="$deployment" ENVIRONMENT="$environment" IMAGE="$image" EXPECTED_DIGEST="$expected_digest" \
      GHCR_USERNAME=test-actor GHCR_TOKEN=test-package-token \
      bash "$repository_root/.github/scripts/deploy-service.sh"
  ) > "$case_dir/output" 2>&1 || status=$?
  assert_equal "$status" "$expected_status" "$environment/$injected_failure exit status $status"
  [[ $(cat -- "$case_dir/output") != *test-package-token* ]] || fail 'registry credential printed'
  [[ $(cat -- "$case_dir/output") != *test-ssh-private-key* ]] || fail 'SSH credential printed'
  for name in .env config.yml catalog; do
    directory=$deployment
    [[ "$name" != catalog ]] || directory="$deployment/state"
    cmp -- "$directory/$name" "$case_dir/prepared/$name" || fail "prepared $name changed"
  done
  while IFS= read -r directory; do
    [[ ! -e "$directory" ]] || fail 'temporary SSH credentials remain'
  done < "$case_dir/ssh-directories"
  if [[ -f "$case_dir/docker-directories" ]]; then
    while IFS= read -r directory; do
      [[ ! -e "$directory" ]] || fail 'temporary Docker credentials remain'
    done < "$case_dir/docker-directories"
  fi
  shopt -s nullglob
  local leftovers=("$deployment"/.service-release.*)
  [[ ${#leftovers[@]} = 0 ]] || fail 'temporary remote release directory remains'
  if [[ "$expected_status" = 0 ]]; then
    assert_equal "$(cat -- "$deployment/release.env")" "RFS_IMAGE=$image" 'successful selector does not select the build tag'
    assert_equal "$(cat -- "$case_dir/docker-events")" $'login\npull\ninspect\nwait' 'success did not check the pulled digest before waiting'
    assert_equal "$(cat -- "$case_dir/project")" "remote-fs-$environment" 'environment project mismatch'
    cmp -- "$deployment/compose.yml" "$repository_root/deployments/service/compose.yml" || fail 'wrong Compose file deployed'
  else
    assert_equal "$(cat -- "$deployment/release.env")" "RFS_IMAGE=$previous_image" 'failure advanced the release selector'
    case "$injected_failure" in
      login) assert_equal "$(cat -- "$case_dir/docker-events")" login 'login failure reached Compose' ;;
      pull) assert_equal "$(cat -- "$case_dir/docker-events")" $'login\npull' 'pull failure reached inspect' ;;
      inspect|mismatch|image-id) assert_equal "$(cat -- "$case_dir/docker-events")" $'login\npull\ninspect' 'digest failure reached Compose' ;;
      wait) assert_equal "$(cat -- "$case_dir/docker-events")" $'login\npull\ninspect\nwait' 'health failure bypassed digest verification' ;;
      ssh|scp) [[ ! -s "$case_dir/docker-events" ]] || fail 'transport failure reached Docker' ;;
    esac
  fi
  printf 'PASS: %s/%s\n' "$environment" "${injected_failure:-success}"
}

unusual_name="staging path '\$(touch injected)' \";;&[]"
run_case staging '' 0 "$unusual_name"
run_case prod '' 0 'production path'
assert_equal "$(cat -- "$test_tmp/case-1/image")" "$(cat -- "$test_tmp/case-2/image")" 'environments did not deploy the same tag'
[[ $(cat -- "$test_tmp/case-1/project") != "$(cat -- "$test_tmp/case-2/project")" ]] || fail 'environment projects collide'
[[ ! -e "$test_tmp/injected" ]] || fail 'deployment path was interpreted as a shell command'
run_case staging login 42 'login failure'
run_case staging pull 43 'pull failure'
run_case staging inspect 46 'inspect failure'
run_case staging mismatch 1 'digest mismatch'
run_case staging image-id 1 'image ID is not a repository digest'
run_case staging wait 44 'health failure'
run_case staging ssh 45 'SSH failure'
run_case staging scp 45 'SCP failure'
printf '%s\n' 'PASS: deployment status, selectors, environment isolation and credential cleanup'
