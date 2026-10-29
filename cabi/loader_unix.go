//go:build linux || darwin

package cabi

import "github.com/ebitengine/purego"

func openNativeLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
}

func lookupNativeSymbol(library uintptr, name string) (uintptr, error) {
	return purego.Dlsym(library, name)
}

func closeNativeLibrary(library uintptr) error {
	return purego.Dlclose(library)
}

func addDLLSearchPath(string) {}
