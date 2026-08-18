// Package cliproduct declares that the importing binary is one of the BV-BRC
// p3-* command-line tools, so that its requests are sent as
// "bvbrc-cli-go/<version>" rather than as the library's own name.
//
// It is imported for its side effect only:
//
//	import _ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
//
// Every command that talks to a service imports it -- except p3-login, which
// declares version.AuthProduct instead. TestEveryCommandDeclaresAProduct (in
// cliproduct_test.go) enforces that, so a new command cannot quietly ship
// announcing itself as the library.
package cliproduct

import "github.com/BV-BRC/BV-BRC-Go-SDK/version"

func init() {
	version.SetProduct(version.CLIProduct)
}
