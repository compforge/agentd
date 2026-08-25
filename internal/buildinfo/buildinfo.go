package buildinfo

import "fmt"

// Version, Revision, and BuildTime are overridden by release builds through
// linker flags. Local builds keep explicit diagnostic defaults.
var (
	Version   = "dev"
	Revision  = "unknown"
	BuildTime = "unknown"
)

func String(name string) string {
	return fmt.Sprintf("%s version %s (revision %s, built %s)", name, Version, Revision, BuildTime)
}
