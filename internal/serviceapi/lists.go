package serviceapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
)

const maxListEntries = 100000

func (c *Client) VolumesPage(ctx context.Context, after string) (VolumeList, error) {
	var result VolumeList
	if after != "" {
		if err := validateID(after); err != nil {
			return result, err
		}
	}
	query := url.Values{"limit": {"50"}}
	if after != "" {
		query.Set("after", after)
	}
	err := c.exchange(ctx, http.MethodGet, "/volumes?"+query.Encode(), nil, &result)
	return result, err
}

func (c *Client) ListVolumes(ctx context.Context) ([]Volume, error) {
	result := make([]Volume, 0)
	after := ""
	for pages := 0; pages <= maxListEntries/50; pages++ {
		page, err := c.VolumesPage(ctx, after)
		if err != nil {
			return nil, err
		}
		if len(page.Volumes) > 50 || len(result)+len(page.Volumes) > maxListEntries {
			return nil, errors.New("volume list exceeds limit")
		}
		result = append(result, page.Volumes...)
		if page.NextAfter == "" {
			return result, nil
		}
		if page.NextAfter <= after || validateID(page.NextAfter) != nil {
			return nil, errors.New("invalid volume continuation")
		}
		after = page.NextAfter
	}
	return nil, errors.New("volume pagination exceeds limit")
}

func (c *Client) UsersPage(ctx context.Context, after string) (UserList, error) {
	var result UserList
	if after != "" {
		if err := validateID(after); err != nil {
			return result, err
		}
	}
	query := url.Values{"limit": {"50"}}
	if after != "" {
		query.Set("after", after)
	}
	err := c.exchange(ctx, http.MethodGet, "/admin/users?"+query.Encode(), nil, &result)
	return result, err
}

func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	result := make([]User, 0)
	after := ""
	for pages := 0; pages <= maxListEntries/50; pages++ {
		page, err := c.UsersPage(ctx, after)
		if err != nil {
			return nil, err
		}
		if len(page.Users) > 50 || len(result)+len(page.Users) > maxListEntries {
			return nil, errors.New("user list exceeds limit")
		}
		result = append(result, page.Users...)
		if page.NextAfter == "" {
			return result, nil
		}
		if page.NextAfter <= after || validateID(page.NextAfter) != nil {
			return nil, errors.New("invalid user continuation")
		}
		after = page.NextAfter
	}
	return nil, errors.New("user pagination exceeds limit")
}

func (c *Client) UserVolumesPage(ctx context.Context, id, after string) (UserVolumeList, error) {
	var result UserVolumeList
	if err := validateID(id); err != nil {
		return result, err
	}
	if after != "" {
		if len(after) > 512 {
			return result, errors.New("user volume continuation exceeds limit")
		}
	}
	query := url.Values{"limit": {"50"}}
	if after != "" {
		query.Set("after", after)
	}
	err := c.exchange(ctx, http.MethodGet, "/admin/users/"+id+"/volumes?"+query.Encode(), nil, &result)
	return result, err
}

func (c *Client) ListUserVolumes(ctx context.Context, id string) ([]UserVolumeSummary, error) {
	result := make([]UserVolumeSummary, 0)
	after := ""
	seen := make(map[string]bool)
	for pages := 0; pages <= maxListEntries/50; pages++ {
		page, err := c.UserVolumesPage(ctx, id, after)
		if err != nil {
			return nil, err
		}
		if len(page.Volumes) > 50 || len(result)+len(page.Volumes) > maxListEntries {
			return nil, errors.New("user volume list exceeds limit")
		}
		result = append(result, page.Volumes...)
		if page.NextAfter == "" {
			return result, nil
		}
		if len(page.NextAfter) > 512 || seen[page.NextAfter] {
			return nil, errors.New("invalid user volume continuation")
		}
		seen[page.NextAfter] = true
		after = page.NextAfter
	}
	return nil, errors.New("user volume pagination exceeds limit")
}
