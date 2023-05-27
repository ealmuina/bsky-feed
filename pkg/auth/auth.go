package auth

import "github.com/bluesky-social/indigo/xrpc"

type AuthConfig struct {
	Did    string
	Client *xrpc.Client
}
