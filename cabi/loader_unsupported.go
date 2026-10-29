//go:build !windows && !linux && !darwin

package cabi

import "fmt"

func openNativeLibrary(path string) (uintptr, error) {
	return 0, fmt.Errorf("dynamic native loading is not supported on this platform: %s", path)
}

func lookupNativeSymbol(uintptr, string) (uintptr, error) {
	return 0, fmt.Errorf("dynamic native loading is not supported on this platform")
}

func closeNativeLibrary(uintptr) error { return nil }
func addDLLSearchPath(string)          {}
