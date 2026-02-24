package web

import "embed"

//go:embed all:static
var FrontendFS embed.FS
