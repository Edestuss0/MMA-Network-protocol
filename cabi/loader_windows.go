//go:build windows

package cabi

import (
	"os"
	"strings"
	"syscall"
)

func openNativeLibrary(path string) (uintptr, error) {
	handle, err := syscall.LoadLibrary(path)
	return uintptr(handle), err
}

func lookupNativeSymbol(library uintptr, name string) (uintptr, error) {
	return syscall.GetProcAddress(syscall.Handle(library), name)
}

func closeNativeLibrary(library uintptr) error {
	return syscall.FreeLibrary(syscall.Handle(library))
}

func addDLLSearchPath(directory string) {
	if directory == "" {
		return
	}
	current := os.Getenv("PATH")
	for _, part := range strings.Split(current, string(os.PathListSeparator)) {
		if strings.EqualFold(part, directory) {
			return
		}
	}
	_ = os.Setenv("PATH", directory+string(os.PathListSeparator)+current)
}
