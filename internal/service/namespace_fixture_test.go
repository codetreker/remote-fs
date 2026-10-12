package service

import (
	"context"

	"github.com/codetreker/remote-fs/internal/management"
)

func fixtureIssuer(store *management.Store) func(context.Context, management.ID) (management.ActionNamespace, error) {
	return store.IssueNamespace
}

func issueServiceFixtureNamespace(ctx context.Context, store *management.Store, actor management.ID) (management.ActionNamespace, error) {
	return store.IssueNamespace(ctx, actor)
}
