package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

const testAuthPassword = "administrator-test-password"

var testAuthHashOnce sync.Once
var testAuthHash string
var testAuthHashError error

func fixtureAuthHash(t *testing.T) string {
	t.Helper()
	testAuthHashOnce.Do(func() { testAuthHash, testAuthHashError = HashAdminPassword(testAuthPassword) })
	if testAuthHashError != nil {
		t.Fatal(testAuthHashError)
	}
	return testAuthHash
}

type authFixture struct {
	t        *testing.T
	store    *management.Store
	auth     *Authenticator
	server   *httptest.Server
	client   *http.Client
	admin    management.User
	database string
	options  management.Options
	config   AuthConfig
}

func newAuthFixture(t *testing.T, adjust func(*management.Options, *AuthConfig)) *authFixture {
	t.Helper()
	temporaryRoot, err := filepath.Abs("../../.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(temporaryRoot, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(temporaryRoot, "auth-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	f := &authFixture{t: t, database: filepath.Join(dir, "management.sqlite"), options: management.DefaultOptions()}
	f.options.Create = true
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.auth.Handler().ServeHTTP(w, r) }))
	t.Cleanup(f.server.Close)
	f.config = DefaultAuthConfig(f.server.URL)
	if adjust != nil {
		adjust(&f.options, &f.config)
	}
	f.store, err = management.Open(context.Background(), f.database, f.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.admin, err = f.store.BootstrapAdmin(context.Background(), "root", "Administrator", fixtureAuthHash(t))
	if err != nil {
		t.Fatal(err)
	}
	f.auth, err = NewAuthenticator(f.store, f.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.auth.Close(); err != nil {
			t.Error(err)
		}
	})
	browserClient := *f.server.Client()
	f.client = &browserClient
	f.client.Jar, _ = cookiejar.New(nil)
	f.client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return f
}

func (f *authFixture) request(client *http.Client, method, path string, body any, headers map[string]string) *http.Response {
	f.t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, f.server.URL+"/api/service/v1/auth"+path, bytes.NewReader(data))
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

func readAuthResponse(t *testing.T, response *http.Response, status int, target any) {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("HTTP %d, wanted %d: %s", response.StatusCode, status, body)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("authentication response is cacheable")
	}
	if target != nil {
		if err := json.Unmarshal(body, target); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *authFixture) login() (string, *http.Response) {
	f.t.Helper()
	response := f.request(f.client, "POST", "/login", map[string]string{"loginName": "root", "password": testAuthPassword}, map[string]string{"Origin": f.server.URL, "X-RFS-Request": "1"})
	var body struct {
		CSRFToken string `json:"csrfToken"`
	}
	readAuthResponse(f.t, response, http.StatusOK, &body)
	return body.CSRFToken, response
}

func (f *authFixture) cliLogin() (credentialResponse, string) {
	f.t.Helper()
	response := f.request(f.server.Client(), "POST", "/cli-credentials", map[string]string{"loginName": "root", "password": testAuthPassword, "label": "test CLI"}, nil)
	var body struct {
		Credential credentialResponse `json:"credential"`
		Secret     string             `json:"secret"`
	}
	readAuthResponse(f.t, response, http.StatusCreated, &body)
	if len(response.Cookies()) != 0 {
		f.t.Fatal("CLI login set a browser cookie")
	}
	return body.Credential, body.Secret
}

func credentialHeader(id management.ID, secret string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + string(id) + "." + secret}
}

func (f *authFixture) bearerPrincipal(id management.ID, secret string) Principal {
	f.t.Helper()
	request := httptest.NewRequest("GET", f.server.URL+"/", nil)
	request.Header.Set("Authorization", "Bearer "+string(id)+"."+secret)
	p, err := f.auth.Authenticate(context.Background(), request)
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

func TestBrowserSessionCookieCSRFReloadSecondTabAndReopen(t *testing.T) {
	f := newAuthFixture(t, nil)
	csrf, response := f.login()
	cookies := response.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("got %d cookies", len(cookies))
	}
	cookie := cookies[0]
	if cookie.Name != authCookieName || !cookie.Secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.Domain != "" {
		t.Fatalf("unsafe cookie attributes: name=%s secure=%v httpOnly=%v sameSite=%v path=%s domain=%s", cookie.Name, cookie.Secure, cookie.HttpOnly, cookie.SameSite, cookie.Path, cookie.Domain)
	}
	otherTab := *f.client
	for _, client := range []*http.Client{f.client, &otherTab} {
		response := f.request(client, "GET", "/session", nil, nil)
		var body struct {
			CSRFToken string `json:"csrfToken"`
		}
		readAuthResponse(t, response, http.StatusOK, &body)
		if body.CSRFToken != csrf {
			t.Fatal("session token changed across tabs or reload")
		}
	}
	id := f.store.DatabaseID()
	if err := f.auth.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.options.Create = false
	f.options.ExpectedDatabaseID = id
	var err error
	f.store, err = management.Open(context.Background(), f.database, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.auth, err = NewAuthenticator(f.store, f.config)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		CSRFToken string `json:"csrfToken"`
	}
	readAuthResponse(t, f.request(f.client, "GET", "/session", nil, nil), http.StatusOK, &body)
	if body.CSRFToken != csrf {
		t.Fatal("session token did not survive restart")
	}
}

func TestBrowserMutationRequiresExactOriginAndSessionCSRF(t *testing.T) {
	f := newAuthFixture(t, nil)
	csrf, _ := f.login()
	for _, headers := range []map[string]string{{"X-CSRF-Token": csrf}, {"Origin": f.server.URL + "/", "X-CSRF-Token": csrf}, {"Origin": f.server.URL}, {"Origin": f.server.URL, "X-CSRF-Token": "another-session"}} {
		readAuthResponse(t, f.request(f.client, "POST", "/logout", nil, headers), http.StatusForbidden, nil)
	}
	readAuthResponse(t, f.request(f.client, "POST", "/logout", nil, map[string]string{"Origin": f.server.URL, "X-CSRF-Token": csrf}), http.StatusNoContent, nil)
	readAuthResponse(t, f.request(f.client, "GET", "/session", nil, nil), http.StatusUnauthorized, nil)
}

func TestLoginRejectsCrossOriginDuplicateKeysAndAmbiguousCredentials(t *testing.T) {
	f := newAuthFixture(t, nil)
	for _, headers := range []map[string]string{{"Origin": "https://another.example", "X-RFS-Request": "1"}, {"Origin": f.server.URL}} {
		expected := http.StatusForbidden
		if headers["Origin"] == f.server.URL {
			expected = http.StatusBadRequest
		}
		readAuthResponse(t, f.request(f.client, "POST", "/login", map[string]string{"loginName": "root", "password": testAuthPassword}, headers), expected, nil)
	}
	request, err := http.NewRequest("POST", f.server.URL+"/api/service/v1/auth/login", strings.NewReader(`{"loginName":"root","loginName":"root","password":"administrator-test-password"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", f.server.URL)
	request.Header.Set("X-RFS-Request", "1")
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	readAuthResponse(t, response, http.StatusBadRequest, nil)
	credential, secret := f.cliLogin()
	f.login()
	readAuthResponse(t, f.request(f.client, "GET", "/session", nil, credentialHeader(credential.ID, secret)), http.StatusUnauthorized, nil)
}

func TestCredentialRotationReplayGraceStableBindingAndRevocation(t *testing.T) {
	f := newAuthFixture(t, func(options *management.Options, _ *AuthConfig) { options.RotationGrace = 200 * time.Millisecond })
	credential, secret := f.cliLogin()
	p := f.bearerPrincipal(credential.ID, secret)
	originalBinding, err := f.auth.AccessBinding(WithPrincipal(context.Background(), p))
	if err != nil {
		t.Fatal(err)
	}
	output, cancel, err := f.auth.OutputContext(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	pending, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	action, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	activateBeforePrepare := activateRotationRequest{ActionID: action, ExpectedGeneration: "1"}
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/activate", activateBeforePrepare, credentialHeader(credential.ID, pending)), http.StatusUnauthorized, nil)
	unverifiedGeneration := activateBeforePrepare
	unverifiedGeneration.ExpectedGeneration = "999"
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/activate", unverifiedGeneration, credentialHeader(credential.ID, pending)), http.StatusUnauthorized, nil)
	mismatchedID, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(mismatchedID)+"/rotation/activate", activateBeforePrepare, credentialHeader(credential.ID, pending)), http.StatusForbidden, nil)
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/activate", map[string]string{"actionId": "not-an-action", "expectedGeneration": "1"}, credentialHeader(credential.ID, pending)), http.StatusBadRequest, nil)
	prepare := prepareRotationRequest{ActionID: action, ExpectedGeneration: "1", Secret: pending}
	for range 2 {
		readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/prepare", prepare, credentialHeader(credential.ID, secret)), http.StatusOK, nil)
	}
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, pending)), http.StatusUnauthorized, nil)
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/activate", unverifiedGeneration, credentialHeader(credential.ID, pending)), http.StatusConflict, nil)
	var stillPending credentialResponse
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, secret)), http.StatusOK, &stillPending)
	if stillPending.Generation != "1" || stillPending.PendingActionID != action {
		t.Fatal("generation rejection activated or discarded pending credential")
	}

	changed := prepare
	changed.Secret, _ = randomAuthSecret()
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/prepare", changed, credentialHeader(credential.ID, secret)), http.StatusConflict, nil)
	activate := activateRotationRequest{ActionID: action, ExpectedGeneration: "1"}
	for range 2 {
		var result credentialResponse
		readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials/"+string(credential.ID)+"/rotation/activate", activate, credentialHeader(credential.ID, pending)), http.StatusOK, &result)
		if result.Generation != "2" || !result.ExpiresAt.Equal(credential.ExpiresAt) {
			t.Fatalf("rotation changed expiry or generation: %+v", result)
		}
	}
	newPrincipal := f.bearerPrincipal(credential.ID, pending)
	newBinding, err := f.auth.AccessBinding(WithPrincipal(context.Background(), newPrincipal))
	if err != nil || newBinding != originalBinding {
		t.Fatalf("rotation changed access binding: %v, %+v", err, newBinding)
	}
	select {
	case <-output.Done():
		t.Fatal("rotation canceled existing output")
	default:
	}
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, secret)), http.StatusOK, nil)
	timer := time.NewTimer(250 * time.Millisecond)
	<-timer.C
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, secret)), http.StatusUnauthorized, nil)
	readAuthResponse(t, f.request(f.server.Client(), "DELETE", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, pending)), http.StatusNoContent, nil)
	select {
	case <-output.Done():
	case <-time.After(time.Second):
		t.Fatal("credential revocation did not cancel output")
	}
	if err := f.auth.CheckCurrent(context.Background(), newPrincipal); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revocation check: %v", err)
	}
}

func TestPasswordRecoveryInvalidatesSessionsAndCredentials(t *testing.T) {
	f := newAuthFixture(t, nil)
	f.login()
	credential, secret := f.cliLogin()
	old := f.bearerPrincipal(credential.ID, secret)
	output, cancel, err := f.auth.OutputContext(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	updated, err := f.store.ResetAdminPassword(context.Background(), f.admin.ID, fixtureAuthHash(t))
	if err != nil {
		t.Fatal(err)
	}
	if updated.ID != old.UserID || updated.AuthEpoch != old.AuthEpoch+1 {
		t.Fatal("password recovery changed subject or failed to advance epoch")
	}
	readAuthResponse(t, f.request(f.client, "GET", "/session", nil, nil), http.StatusUnauthorized, nil)
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, secret)), http.StatusUnauthorized, nil)
	select {
	case <-output.Done():
	case <-time.After(time.Second):
		t.Fatal("password recovery did not cancel output")
	}
	newer, newSecret := f.cliLogin()
	current := f.bearerPrincipal(newer.ID, newSecret)
	if current.UserID != old.UserID || current.AuthenticationID == old.AuthenticationID {
		t.Fatal("recovery did not retain subject and replace access identity")
	}
}

func TestAdminRoleCannotAccessAnotherOwnersFiles(t *testing.T) {
	f := newAuthFixture(t, nil)
	other, err := f.store.AddAdmin(context.Background(), "other", "Other administrator", fixtureAuthHash(t))
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := issueServiceFixtureNamespace(context.Background(), f.store, other.ID)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := f.store.CreateVolume(context.Background(), other.ID, management.ActionKey{Namespace: namespace.Sequence, Nonce: "create"}, "Private", limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	credential, secret := f.cliLogin()
	p := f.bearerPrincipal(credential.ID, secret)
	if err := f.auth.CheckFileAccess(context.Background(), p, operation.VolumeID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("admin file access: %v", err)
	}
	authorizer := f.auth.Authorizer(operation.VolumeID)
	if err := authorizer.Authorize(WithPrincipal(context.Background(), p), authz.AccessRequest{Volume: string(operation.VolumeID), Operation: storage.OpVolumeList}); !errors.Is(err, authz.ErrDenied) {
		t.Fatalf("semantic admin access: %v", err)
	}
	ownNamespace, err := issueServiceFixtureNamespace(context.Background(), f.store, f.admin.ID)
	if err != nil {
		t.Fatal(err)
	}
	own, err := f.store.CreateVolume(context.Background(), f.admin.ID, management.ActionKey{Namespace: ownNamespace.Sequence, Nonce: "own"}, "Own", limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.auth.CheckFileAccess(context.Background(), p, own.VolumeID); err != nil {
		t.Fatal(err)
	}
	p.AllOwnedVolumes = false
	p.VolumeIDs = []management.ID{operation.VolumeID}
	if err := f.auth.CheckFileAccess(context.Background(), p, own.VolumeID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("scope did not fence owned runtime admission: %v", err)
	}
}

func TestCLIRejectsScopeWithoutExplicitSelectionAndCannotMintBroaderCredential(t *testing.T) {
	f := newAuthFixture(t, nil)
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials", map[string]any{"loginName": "root", "password": testAuthPassword, "label": "ambiguous", "scopes": []string{ScopeFiles}}, nil), http.StatusBadRequest, nil)
	credential, secret := f.cliLogin()
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/cli-credentials", map[string]string{"loginName": "root", "password": testAuthPassword, "label": "replacement"}, credentialHeader(credential.ID, secret)), http.StatusForbidden, nil)
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/session", nil, credentialHeader(credential.ID, secret)), http.StatusForbidden, nil)
	second, _ := f.cliLogin()
	readAuthResponse(t, f.request(f.server.Client(), "DELETE", "/cli-credentials/"+string(second.ID), nil, credentialHeader(credential.ID, secret)), http.StatusForbidden, nil)
}

func TestLoginThrottleHasBoundedBucketsAndUniformFailure(t *testing.T) {
	f := newAuthFixture(t, func(_ *management.Options, config *AuthConfig) { config.LoginAttempts = 2; config.LoginBuckets = 4 })
	for _, login := range []string{"root", "absent"} {
		readAuthResponse(t, f.request(f.server.Client(), "POST", "/login", map[string]string{"loginName": login, "password": "wrong-password"}, map[string]string{"Origin": f.server.URL, "X-RFS-Request": "1"}), http.StatusUnauthorized, nil)
	}
	readAuthResponse(t, f.request(f.server.Client(), "POST", "/login", map[string]string{"loginName": "root", "password": testAuthPassword}, map[string]string{"Origin": f.server.URL, "X-RFS-Request": "1"}), http.StatusTooManyRequests, nil)
	if len(f.auth.logins.buckets) > f.config.LoginBuckets {
		t.Fatal("login buckets exceeded configured capacity")
	}
	limits := loginLimits{buckets: make(map[[32]byte]loginBucket), maxBuckets: 2, maxAttempts: 2, window: time.Minute}
	request := httptest.NewRequest("POST", "https://example.invalid", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	if err := limits.admit(request, "first", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := limits.admit(request, "second", time.Now()); !errors.Is(err, ErrAuthCapacity) {
		t.Fatalf("full bucket capacity: %v", err)
	}
	if err := limits.admit(request, "second", time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("expired buckets retained capacity: %v", err)
	}
}

func TestPasswordVerificationQueueAndOutputContextsAreBounded(t *testing.T) {
	admission := newPasswordAdmission(time.Second)
	first, err := admission.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	second, err := admission.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := admission.acquire(ctx)
			if err == nil {
				release()
				t.Error("waiting password request unexpectedly acquired running slot")
			}
		}()
	}
	deadline := time.Now().Add(time.Second)
	for len(admission.admitted) != 10 {
		if time.Now().After(deadline) {
			t.Fatal("password waiters failed to occupy bounded queue")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := admission.acquire(context.Background()); !errors.Is(err, ErrAuthCapacity) {
		t.Fatalf("password queue capacity: %v", err)
	}
	cancel()
	wg.Wait()
	f := newAuthFixture(t, func(_ *management.Options, config *AuthConfig) {
		config.MaxOutputContexts = 1
		config.MaxOutputsPerAccess = 1
	})
	credential, secret := f.cliLogin()
	p := f.bearerPrincipal(credential.ID, secret)
	_, finish, err := f.auth.OutputContext(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, _, err := f.auth.OutputContext(context.Background(), p); !errors.Is(err, ErrAuthCapacity) {
		t.Fatalf("output context capacity: %v", err)
	}
	finish()
	_, finishAgain, err := f.auth.OutputContext(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	finishAgain()
	if err := f.auth.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.auth.OutputContext(context.Background(), p); !errors.Is(err, ErrAuthClosed) {
		t.Fatalf("output reopened after Close: %v", err)
	}
	f.auth.outputMu.Lock()
	outputs := f.auth.outputCount
	f.auth.outputMu.Unlock()
	if outputs != 0 {
		t.Fatalf("Close retained %d output scopes", outputs)
	}
}

func TestAuthenticationStoreFailureIsUnavailableAndCancelsOutput(t *testing.T) {
	f := newAuthFixture(t, nil)
	credential, secret := f.cliLogin()
	p := f.bearerPrincipal(credential.ID, secret)
	output, cancel, err := f.auth.OutputContext(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	readAuthResponse(t, f.request(f.server.Client(), "GET", "/cli-credentials/"+string(credential.ID), nil, credentialHeader(credential.ID, secret)), http.StatusServiceUnavailable, nil)
	if err := f.auth.CheckCurrent(context.Background(), p); err == nil || errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("store failure concealed as identity failure: %v", err)
	}
	select {
	case <-output.Done():
	case <-time.After(time.Second):
		t.Fatal("store failure did not cancel output")
	}
}

func TestPasswordHashRejectsUnsupportedPolicyAndConfigRejectsInventedDefaults(t *testing.T) {
	hash := fixtureAuthHash(t)
	for _, input := range []struct {
		password string
		match    bool
	}{{testAuthPassword, true}, {"wrong-password", false}} {
		match, err := verifyAdminPassword(input.password, hash)
		if err != nil || match != input.match {
			t.Fatalf("verify password: %v, %v", match, err)
		}
	}
	if _, err := verifyAdminPassword(testAuthPassword, strings.Replace(hash, "m=65536", "m=1073741824", 1)); err == nil {
		t.Fatal("unbounded Argon2 memory policy accepted")
	}
	for _, password := range []string{"short", strings.Repeat("x", 1025)} {
		if _, err := HashAdminPassword(password); err == nil {
			t.Fatal("invalid password length accepted")
		}
	}
	f := newAuthFixture(t, nil)
	if _, err := NewAuthenticator(f.store, AuthConfig{Origin: f.server.URL}); err == nil {
		t.Fatal("zero authentication limits accepted")
	}
	for _, origin := range []string{"http://example.invalid", "https://example.invalid/", "https://user@example.invalid", "https://example.invalid?x=1"} {
		if _, err := NewAuthenticator(f.store, DefaultAuthConfig(origin)); err == nil {
			t.Fatalf("unsafe origin accepted: %s", origin)
		}
	}
}

func TestOutputRequiresCurrentIdentityAndSupportedSocketDeadline(t *testing.T) {
	f := newAuthFixture(t, nil)
	credential, secret := f.cliLogin()
	p := f.bearerPrincipal(credential.ID, secret)
	if err := f.auth.CheckOutput(context.Background(), httptest.NewRecorder()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("output without identity: %v", err)
	}
	if err := f.auth.CheckOutput(WithPrincipal(context.Background(), p), httptest.NewRecorder()); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("unsupported output deadline concealed: %v", err)
	}
	writer := &authDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	if err := f.auth.CheckOutput(WithPrincipal(context.Background(), p), writer); err != nil {
		t.Fatal(err)
	}
	if writer.deadline.IsZero() || writer.deadline.After(time.Now().Add(f.config.OutputWriteTimeout+time.Second)) {
		t.Fatalf("missing bounded deadline: %s", writer.deadline)
	}
	if err := f.store.RevokeCredential(context.Background(), p.UserID, p.AuthenticationID); err != nil {
		t.Fatal(err)
	}
	if err := f.auth.CheckOutput(WithPrincipal(context.Background(), p), writer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked output: %v", err)
	}
}

type authDeadlineWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *authDeadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func TestBrowserCanLoginWithExpiredCookie(t *testing.T) {
	f := newAuthFixture(t, nil)
	f.login()
	serverURL, err := url.Parse(f.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	jarCookies := f.client.Jar.Cookies(serverURL)
	if len(jarCookies) != 1 {
		t.Fatal("missing session cookie")
	}
	id, _, err := parseBearer("Bearer " + jarCookies[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RevokeSession(context.Background(), f.admin.ID, id); err != nil {
		t.Fatal(err)
	}
	readAuthResponse(t, f.request(f.client, "GET", "/session", nil, nil), http.StatusUnauthorized, nil)
	f.login()
	readAuthResponse(t, f.request(f.client, "GET", "/session", nil, nil), http.StatusOK, nil)
}

func TestMalformedAuthenticationAndMiddlewareNeverInventIdentity(t *testing.T) {
	f := newAuthFixture(t, nil)
	id, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	secret, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []string{"Basic password", "Bearer", "Bearer  " + string(id) + "." + secret, "Bearer invalid." + secret, "Bearer " + string(id) + ".short", "Bearer " + string(id) + "." + secret + "="} {
		request := httptest.NewRequest("GET", f.server.URL, nil)
		request.Header.Set("Authorization", header)
		if _, err := f.auth.Authenticate(context.Background(), request); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("malformed auth %q: %v", strings.Split(header, " ")[0], err)
		}
	}
	request := httptest.NewRequest("GET", f.server.URL, nil)
	request.Header.Add("Authorization", "Bearer "+string(id)+"."+secret)
	request.Header.Add("Authorization", "Bearer "+string(id)+"."+secret)
	if _, err := f.auth.Authenticate(context.Background(), request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("duplicate authorization: %v", err)
	}
	request.Header.Del("Authorization")
	request.Header.Set("Cookie", authCookieName+"="+string(id)+"."+secret+"; "+authCookieName+"="+string(id)+"."+secret)
	if _, err := f.auth.Authenticate(context.Background(), request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("duplicate cookies: %v", err)
	}
	request.Header.Del("Cookie")
	if _, err := f.auth.AccessBinding(context.Background()); !errors.Is(err, ErrUnauthenticated) || !errors.Is(err, authz.ErrDenied) {
		t.Fatalf("binding without identity: %v", err)
	}
	handler := f.auth.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unauthenticated middleware invoked handler") }))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("middleware authentication status: %d", recorder.Code)
	}
	credential, secret := f.cliLogin()
	request.Header.Set("Authorization", "Bearer "+string(credential.ID)+"."+secret)
	handler = f.auth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := CurrentPrincipal(r.Context())
		if !ok || p.UserID != f.admin.ID {
			t.Error("middleware did not establish verified owner")
		}
		if err := f.auth.RequireScope(r.Context(), p, ScopeVolumesRead); err != nil {
			t.Error(err)
		}
		if err := f.auth.RequireScope(r.Context(), p, "unknown"); !errors.Is(err, ErrForbidden) {
			t.Errorf("unknown scope: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("authenticated middleware status: %d", recorder.Code)
	}
}
