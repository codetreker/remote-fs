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
)

const maxResponseBytes = 8 << 20
const maxDirectoryResponseBytes = 64 << 20

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

// HTTPClient keeps credentials bound to this service origin.
func (c *Client) HTTPClient() *http.Client {
	clone := *c.http
	transport := clone.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = credentialTransport{transport: transport, base: c.base, credential: c.credential}
	return &clone
}

func (c *Client) exchange(ctx context.Context, method, path string, input, output any) error {
	return c.exchangeWithLimit(ctx, method, path, input, output, maxResponseBytes)
}

func (c *Client) exchangeWithLimit(ctx context.Context, method, path string, input, output any, maximum int64) error {
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
	data, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > maximum {
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

func (c *Client) CreateVolume(ctx context.Context, input CreateVolumeRequest) (Volume, error) {
	var result Volume
	err := c.exchange(ctx, http.MethodPost, "/volumes", input, &result)
	return result, err
}

func (c *Client) RenameVolume(ctx context.Context, id string, input RenameVolumeRequest) (Volume, error) {
	var result Volume
	if err := validateID(id); err != nil {
		return result, err
	}
	err := c.exchange(ctx, http.MethodPatch, "/volumes/"+id, input, &result)
	if err == nil && result.ID != id {
		err = errors.New("rename response selected another volume")
	}
	return result, err
}

func (c *Client) DeleteVolume(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	return c.exchange(ctx, http.MethodDelete, "/volumes/"+id, nil, nil)
}

func (c *Client) ListFiles(ctx context.Context, id, pathBase64 string) (FileDirectory, error) {
	var result FileDirectory
	if err := validateID(id); err != nil {
		return result, err
	}
	path, err := base64.StdEncoding.Strict().DecodeString(pathBase64)
	if err != nil || base64.StdEncoding.EncodeToString(path) != pathBase64 {
		return result, errors.New("invalid directory path token")
	}
	query := url.Values{"path": {pathBase64}}
	err = c.exchangeWithLimit(ctx, http.MethodGet, "/volumes/"+id+"/files?"+query.Encode(), nil, &result, maxDirectoryResponseBytes)
	if err != nil {
		return result, err
	}
	if result.PathBase64 != pathBase64 {
		return result, errors.New("directory response selected another path")
	}
	prefix := string(path)
	if prefix != "" {
		prefix += "/"
	}
	for index := range result.Entries {
		name, err := base64.StdEncoding.Strict().DecodeString(result.Entries[index].NameBase64)
		if err != nil {
			return result, err
		}
		result.Entries[index].PathBase64 = base64.StdEncoding.EncodeToString([]byte(prefix + string(name)))
	}
	return result, nil
}
