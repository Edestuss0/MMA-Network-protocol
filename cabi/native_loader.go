package cabi

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
	"unicode/utf8"
)

const nativeRepository = "https://github.com/Edestuss0/MMA-Network-protocol"

var nativeHTTPClient = &http.Client{Timeout: 2 * time.Minute}

func nativeLibraryName() string {
	switch runtime.GOOS {
	case "windows":
		return "MMA.dll"
	case "darwin":
		return "libMMA.dylib"
	default:
		return "libMMA.so"
	}
}

func nativeBridgeLibraryName() string {
	switch runtime.GOOS {
	case "windows":
		return "mma_go_bridge.dll"
	case "darwin":
		return "libmma_go_bridge.dylib"
	default:
		return "libmma_go_bridge.so"
	}
}

func locateNativeLibrary() (string, error) {
	if explicitDir := strings.TrimSpace(os.Getenv("MMA_NATIVE_DIR")); explicitDir != "" {
		path := filepath.Join(explicitDir, nativeLibraryName())
		bridgePath := filepath.Join(explicitDir, nativeBridgeLibraryName())
		if err := requireRegularFile(path); err != nil {
			return "", fmt.Errorf("MMA_NATIVE_DIR is set but the native library is unavailable: %w", err)
		}
		if err := requireRegularFile(bridgePath); err != nil {
			return "", fmt.Errorf("MMA_NATIVE_DIR is set but the callback bridge is unavailable: %w", err)
		}
		return path, nil
	}

	supported := (runtime.GOOS == "windows" && runtime.GOARCH == "amd64") ||
		(runtime.GOOS == "linux" && runtime.GOARCH == "amd64") ||
		(runtime.GOOS == "darwin" && (runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64"))
	if !supported {
		return "", fmt.Errorf("MMA native binaries are not published for %s/%s (supported: windows/amd64, linux/amd64, darwin/amd64, darwin/arm64)", runtime.GOOS, runtime.GOARCH)
	}

	version := nativeReleaseVersion()
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("find user cache directory: %w", err)
	}
	versionDir := safeCacheComponent(version)
	cacheDir := filepath.Join(cacheBase, "mma-network-protocol", versionDir, runtime.GOOS+"-"+runtime.GOARCH)
	libraryPath := filepath.Join(cacheDir, nativeLibraryName())
	bridgePath := filepath.Join(cacheDir, nativeBridgeLibraryName())

	if os.Getenv("MMA_NATIVE_REFRESH") != "1" {
		if requireRegularFile(libraryPath) == nil && requireRegularFile(bridgePath) == nil {
			return libraryPath, nil
		}
	}
	if err := downloadNativeRelease(version, cacheDir, libraryPath, bridgePath); err != nil {
		return "", err
	}
	return libraryPath, nil
}

func nativeReleaseVersion() string {
	if value := strings.TrimSpace(os.Getenv("MMA_NATIVE_VERSION")); value != "" {
		return value
	}
	info, ok := debug.ReadBuildInfo()
	if ok {
		for _, dependency := range info.Deps {
			if dependency.Path == "github.com/Edestuss0/MMA-Network-protocol" && dependency.Version != "" && dependency.Version != "(devel)" && dependency.Replace == nil {
				return dependency.Version
			}
		}
	}
	return "latest"
}

func safeCacheComponent(value string) string {
	value = filepath.Base(value)
	if value == "." || value == string(filepath.Separator) || value == "" {
		return "latest"
	}
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(value)
}

func downloadNativeRelease(version, cacheDir, libraryPath, bridgePath string) error {
	assetName := fmt.Sprintf("mma-native-%s-%s.zip", runtime.GOOS, runtime.GOARCH)
	baseURL := nativeRepository + "/releases/"
	if version == "" || version == "latest" {
		baseURL += "latest/download/"
	} else {
		if strings.ContainsAny(version, `/\\`) || !utf8.ValidString(version) {
			return fmt.Errorf("invalid MMA_NATIVE_VERSION %q", version)
		}
		baseURL += "download/" + version + "/"
	}
	assetURL := baseURL + assetName
	checksumURL := assetURL + ".sha256"

	parent := filepath.Dir(cacheDir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create MMA native cache: %w", err)
	}
	tmpDir, err := os.MkdirTemp(parent, ".mma-native-install-*")
	if err != nil {
		return fmt.Errorf("create temporary MMA native directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, assetName)
	if err := downloadFile(assetURL, archivePath, 256<<20); err != nil {
		return fmt.Errorf("download MMA native artifact (%s): %w; publish a matching release tag or set MMA_NATIVE_DIR to a directory containing %s and %s", assetURL, err, nativeLibraryName(), nativeBridgeLibraryName())
	}
	checksumPath := archivePath + ".sha256"
	if err := downloadFile(checksumURL, checksumPath, 4096); err != nil {
		return fmt.Errorf("download MMA native checksum (%s): %w", checksumURL, err)
	}
	if err := verifySHA256(archivePath, checksumPath); err != nil {
		return fmt.Errorf("verify MMA native artifact: %w", err)
	}

	stagedLibrary := filepath.Join(tmpDir, nativeLibraryName())
	stagedBridge := filepath.Join(tmpDir, nativeBridgeLibraryName())
	if err := extractNativeLibrary(archivePath, stagedLibrary, nativeLibraryName()); err != nil {
		return err
	}
	if err := extractNativeLibrary(archivePath, stagedBridge, nativeBridgeLibraryName()); err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return fmt.Errorf("create native cache directory: %w", err)
	}
	if err := installNativeFile(stagedLibrary, libraryPath); err != nil {
		return fmt.Errorf("install MMA native library: %w", err)
	}
	if err := installNativeFile(stagedBridge, bridgePath); err != nil {
		return fmt.Errorf("install MMA callback bridge: %w", err)
	}
	return nil
}

func requireRegularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("%q is not a non-empty regular file", path)
	}
	return nil
}

func installNativeFile(source, destination string) error {
	if err := os.Rename(source, destination); err == nil {
		return nil
	}
	// Windows does not replace an existing destination with Rename.
	if _, err := os.Stat(destination); err != nil {
		return err
	}
	if err := os.Remove(destination); err != nil {
		return err
	}
	return os.Rename(source, destination)
}

func downloadFile(address, destination string, maxSize int64) error {
	request, err := http.NewRequest(http.MethodGet, address, nil)
	if err != nil {
		return err
	}
	request.Header.Set("User-Agent", "MMA-Go-CABI/1")
	response, err := nativeHTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return fmt.Errorf("HTTP %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	if response.ContentLength > maxSize {
		return fmt.Errorf("download is too large (%d bytes)", response.ContentLength)
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(file, io.LimitReader(response.Body, maxSize+1))
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if written > maxSize {
		_ = os.Remove(destination)
		return fmt.Errorf("download exceeds %d bytes", maxSize)
	}
	return nil
}

func verifySHA256(archivePath, checksumPath string) error {
	checksumContent, err := os.ReadFile(checksumPath)
	if err != nil {
		return err
	}
	fields := strings.Fields(string(checksumContent))
	if len(fields) == 0 || len(fields[0]) != 64 {
		return fmt.Errorf("malformed SHA-256 file")
	}
	if _, err := hex.DecodeString(fields[0]); err != nil {
		return fmt.Errorf("malformed SHA-256 digest: %w", err)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, archive); err != nil {
		return err
	}
	if !strings.EqualFold(fields[0], hex.EncodeToString(hash.Sum(nil))) {
		return fmt.Errorf("SHA-256 mismatch")
	}
	return nil
}

func extractNativeLibrary(archivePath, destination, expectedName string) error {
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open MMA native archive: %w", err)
	}
	defer archive.Close()
	for _, file := range archive.File {
		if file.Name != expectedName {
			continue
		}
		if file.FileInfo().IsDir() || file.UncompressedSize64 == 0 || file.UncompressedSize64 > 512<<20 {
			return fmt.Errorf("invalid native library entry %q", file.Name)
		}
		input, err := file.Open()
		if err != nil {
			return err
		}
		output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
		if err != nil {
			input.Close()
			return err
		}
		written, copyErr := io.Copy(output, io.LimitReader(input, (512<<20)+1))
		inputCloseErr := input.Close()
		outputCloseErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		if inputCloseErr != nil {
			return inputCloseErr
		}
		if outputCloseErr != nil {
			return outputCloseErr
		}
		if written == 0 || written > 512<<20 {
			return fmt.Errorf("native library entry has invalid size")
		}
		return nil
	}
	return fmt.Errorf("native archive does not contain %q", expectedName)
}
