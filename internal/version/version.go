package version

// These variables are set at build time via ldflags
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String returns the full version string
func String() string {
	return Version
}

// Full returns a detailed version string including commit and date
func Full() string {
	return Version + " (" + Commit + ") built " + Date
}
