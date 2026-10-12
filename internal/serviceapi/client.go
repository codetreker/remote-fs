package serviceapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 8 << 20

type Credential struct{ ID, Secret string }

type Client struct {
	base       *url.URL
	http       *http.Client
	credential Credential
}

func NewClient(baseURL string, httpClient *http.Client, credential Credential) (*Client, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.New("invalid service URL")
	}
	if base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.RawPath != "" || (base.Path != "" && base.Path != "/") {
		return nil, errors.New("service URL must be an origin without credentials, path, query or fragment")
	}
	if base.Scheme != "https" {
		host := base.Hostname()
		ip := net.ParseIP(host)
		if base.Scheme != "http" || (host != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return nil, errors.New("service URL requires HTTPS")
		}
	}
	if httpClient == nil {
		return nil, errors.New("HTTP client is required")
	}
	if err := credential.check(); err != nil {
		return nil, err
	}
	base.Path = ""
	client := *httpClient
	client.Jar = nil
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: base, http: &client, credential: credential}, nil
}

func (c Credential) check() error {
	if c.ID == "" && c.Secret == "" {
		return nil
	}
	if err := validateID(c.ID); err != nil {
		return errors.New("invalid credential ID")
	}
	secret, err := base64.RawURLEncoding.Strict().DecodeString(c.Secret)
	if err != nil || len(secret) != 32 || base64.RawURLEncoding.EncodeToString(secret) != c.Secret {
		return errors.New("invalid credential secret")
	}
	return nil
}

func validateID(id string) error {
	if len(id) != 32 || strings.ToLower(id) != id {
		return errors.New("invalid resource ID")
	}
	_, err := hex.DecodeString(id)
	return err
}

func (c *Client) WithCredential(credential Credential) (*Client, error) {
	if err := credential.check(); err != nil {
		return nil, err
	}
	clone := *c
	clone.credential = credential
	return &clone, nil
}

type credentialTransport struct {
	transport  http.RoundTripper
	base       *url.URL
	credential Credential
}

func (t credentialTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != t.base.Scheme || request.URL.Host != t.base.Host || request.URL.User != nil || request.URL.Opaque != "" || (request.Host != "" && request.Host != t.base.Host) {
		return nil, errors.New("authenticated request must remain on the service origin")
	}
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Del("Authorization")
	clone.Header.Del("Cookie")
	if t.credential.ID != "" {
		clone.Header.Set("Authorization", "Bearer "+t.credential.ID+"."+t.credential.Secret)
	}
	return t.transport.RoundTrip(clone)
}

// HTTPClient supplies the same origin-bound identity to the existing v5 client.
func (c *Client) HTTPClient() *http.Client {
	clone := *c.http
	transport := clone.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = credentialTransport{transport: transport, base: c.base, credential: c.credential}
	return &clone
}

func (c *Client) VolumeURL(id string) (string, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	return c.base.String() + "/api/volumes/" + id, nil
}

func (c *Client) exchange(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.base.String()+Prefix+path, body)
	if err != nil {
		return errors.New("invalid service request")
	}
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.HTTPClient().Do(request)
	if err != nil {
		// net/http includes the URL in its error; service paths contain no secrets.
		return fmt.Errorf("service request failed: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxResponseBytes {
		return errors.New("service response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem ErrorResponse
		if err := DecodeJSON(data, &problem); err != nil || !validErrorCode(problem.Error.Code) {
			return errors.New("service returned an invalid error response")
		}
		return &APIError{Status: response.StatusCode, Code: problem.Error.Code}
	}
	if output == nil {
		if len(bytes.TrimSpace(data)) != 0 {
			return errors.New("service returned unexpected response data")
		}
		return nil
	}
	if err := DecodeJSON(data, output); err != nil {
		return fmt.Errorf("invalid service response: %w", err)
	}
	return validateResponse(output)
}

func (c *Client) Login(ctx context.Context, input CreateCredentialRequest) (CredentialLogin, error) {
	var result CredentialLogin
	anonymous, err := c.WithCredential(Credential{})
	if err != nil {
		return result, err
	}
	err = anonymous.exchange(ctx, http.MethodPost, "/auth/cli-credentials", input, &result)
	if err == nil {
		err = (Credential{ID: result.Credential.ID, Secret: result.Secret}).check()
	}
	return result, err
}

func (c *Client) Credential(ctx context.Context, id string) (CredentialInfo, error) {
	var result CredentialInfo
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodGet, "/auth/cli-credentials/"+id, nil, &result)
	if err == nil && result.ID != id {
		err = errors.New("credential response selected another identity")
	}
	return result, err
}

func (c *Client) RevokeCredential(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	return c.exchange(ctx, http.MethodDelete, "/auth/cli-credentials/"+id, nil, nil)
}

func (c *Client) PrepareRotation(ctx context.Context, id string, input PrepareRotationRequest) (CredentialInfo, error) {
	var result CredentialInfo
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodPost, "/auth/cli-credentials/"+id+"/rotation/prepare", input, &result)
	if err == nil && result.ID != id {
		err = errors.New("rotation response selected another identity")
	}
	return result, err
}

func (c *Client) ActivateRotation(ctx context.Context, id string, input ActivateRotationRequest) (CredentialInfo, error) {
	var result CredentialInfo
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodPost, "/auth/cli-credentials/"+id+"/rotation/activate", input, &result)
	if err == nil && result.ID != id {
		err = errors.New("rotation response selected another identity")
	}
	return result, err
}

func (c *Client) IssueNamespace(ctx context.Context) (ActionNamespace, error) {
	var result ActionNamespace
	err := c.exchange(ctx, http.MethodPost, "/action-namespaces", struct{}{}, &result)
	return result, err
}

func (c *Client) GetVolume(ctx context.Context, id string) (Volume, error) {
	var result Volume
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodGet, "/volumes/"+id, nil, &result)
	if err == nil && result.ID != id {
		err = errors.New("volume response selected another identity")
	}
	return result, err
}

func (c *Client) CreateVolume(ctx context.Context, input CreateVolumeRequest) (Operation, error) {
	var result Operation
	err := c.exchange(ctx, http.MethodPost, "/volumes", input, &result)
	return result, err
}

func (c *Client) RenameVolume(ctx context.Context, id string, input RenameVolumeRequest) (Operation, error) {
	var result Operation
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodPatch, "/volumes/"+id, input, &result)
	if err == nil && result.VolumeID != id {
		err = errors.New("rename response selected another volume")
	}
	return result, err
}

func (c *Client) SetQuota(ctx context.Context, id string, input SetQuotaRequest) (Operation, error) {
	var result Operation
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodPut, "/volumes/"+id+"/quota", input, &result)
	if err == nil && result.VolumeID != id {
		err = errors.New("quota response selected another volume")
	}
	return result, err
}

func (c *Client) DeleteVolume(ctx context.Context, id string, input DeleteVolumeRequest) (Operation, error) {
	var result Operation
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodDelete, "/volumes/"+id, input, &result)
	if err == nil && result.VolumeID != id {
		err = errors.New("delete response selected another volume")
	}
	return result, err
}

func (c *Client) Operation(ctx context.Context, id string) (Operation, error) {
	var result Operation
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodGet, "/operations/"+id, nil, &result)
	if err == nil && result.ID != id {
		err = errors.New("operation response selected another identity")
	}
	return result, err
}

func (c *Client) FindOperation(ctx context.Context, action Action) (Operation, error) {
	var result Operation
	if _, err := action.Namespace.Uint64(); err != nil {
		return result, err
	}
	query := url.Values{"namespace": {string(action.Namespace)}, "nonce": {action.Nonce}}
	err := c.exchange(ctx, http.MethodGet, "/operations?"+query.Encode(), nil, &result)
	return result, err
}

func (c *Client) WaitOperation(ctx context.Context, id string, interval time.Duration) (Operation, error) {
	if interval <= 0 {
		return Operation{}, errors.New("poll interval must be positive")
	}
	for {
		result, err := c.Operation(ctx, id)
		if err != nil {
			return result, err
		}
		if result.State == "succeeded" || (result.State == "failed" && result.Settled) {
			return result, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
}
