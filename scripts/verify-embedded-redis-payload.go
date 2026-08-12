package main

import (
	"archive/zip"
	"bytes"
	"debug/pe"
	"fmt"
	"io"
	"os"
)

const (
	maxExecutableSize = int64(1 << 30)
	maxPayloadSize    = int64(128 << 20)
)

func main() {
	if len(os.Args) != 3 {
		fatalf("usage: %s <windows-release.zip> <redis-payload.zip>", os.Args[0])
	}
	payload, err := readPayloadLimited(os.Args[2], maxPayloadSize)
	if err != nil {
		fatalf("%v", err)
	}
	archive, err := zip.OpenReader(os.Args[1])
	if err != nil {
		fatalf("open Windows release archive: %v", err)
	}
	defer archive.Close()
	if len(archive.File) != 1 || archive.File[0].Name != "openlist.exe" {
		fatalf("Windows release archive must contain only openlist.exe")
	}
	executable, err := readLimited(archive.File[0], maxExecutableSize)
	if err != nil {
		fatalf("read openlist.exe: %v", err)
	}
	if len(executable) < 2 || executable[0] != 'M' || executable[1] != 'Z' {
		fatalf("openlist.exe does not have an MZ executable header")
	}
	peFile, err := pe.NewFile(bytes.NewReader(executable))
	if err != nil {
		fatalf("openlist.exe is not a valid PE file: %v", err)
	}
	defer peFile.Close()
	if peFile.Machine != pe.IMAGE_FILE_MACHINE_AMD64 {
		fatalf("openlist.exe PE machine is %#x, want AMD64 (%#x)", peFile.Machine, pe.IMAGE_FILE_MACHINE_AMD64)
	}
	optionalHeader, ok := peFile.OptionalHeader.(*pe.OptionalHeader64)
	if !ok || optionalHeader.Magic != 0x20b {
		fatalf("openlist.exe does not have a valid PE32+ optional header")
	}
	if peFile.Characteristics&pe.IMAGE_FILE_EXECUTABLE_IMAGE == 0 {
		fatalf("openlist.exe PE header is not marked as an executable image")
	}
	if peFile.Characteristics&pe.IMAGE_FILE_DLL != 0 {
		fatalf("openlist.exe PE header is marked as a DLL")
	}
	if count := bytes.Count(executable, payload); count != 1 {
		fatalf("openlist.exe must contain the exact Redis payload bytes once, found %d copies", count)
	}
}

func readPayloadLimited(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open Redis payload: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect Redis payload: %w", err)
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("Redis payload exceeds %d-byte verification limit", limit)
	}
	payload, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read Redis payload: %w", err)
	}
	if int64(len(payload)) > limit {
		return nil, fmt.Errorf("Redis payload exceeds %d-byte verification limit", limit)
	}
	return payload, nil
}

func readLimited(file *zip.File, limit int64) ([]byte, error) {
	reader, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("file exceeds %d-byte verification limit", limit)
	}
	return data, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
