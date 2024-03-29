package app

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	defaultPageSize    = 30
	maxScanValueLength = 300
)

// Start the console interface.
func Start(db *KeyValueStore) {
	fmt.Println("Type HELP for list of commands")
	for {
		fmt.Print("> ")
		var input string

		in := bufio.NewReader(os.Stdin)
		input, err := in.ReadString('\n')
		if err != nil {
			fmt.Println("Error: " + err.Error())
			continue
		}
		exit, err := parseAndExecute(input, db)
		if err != nil {
			fmt.Println("Error: " + err.Error())
			continue
		}
		if exit {
			err = db.wal.EmptyBuffer()
			if err != nil {
				fmt.Println("Error: " + err.Error())
			}
			break
		}
	}
}

// parseAndExecute parses the input string into commands and executes them. Returns true if exiting.
func parseAndExecute(input string, db *KeyValueStore) (bool, error) {
	r := regexp.MustCompile(`[^\s"']+|"([^"]*)"|'([^']*)'`)
	parts := r.FindAllString(input, -1)
	if parts == nil {
		return false, errors.New("invalid command")
	}
	for i := 2; i < len(parts); i++ {
		if len(parts[i]) < 3 {
			continue
		}
		if strings.HasPrefix(parts[i], "\"") && strings.HasSuffix(parts[i], "\"") {
			parts[i] = parts[i][1 : len(parts[i])-1]
		}
	}
	switch strings.ToLower(parts[0]) {
	case "help", "?", "commands":
		help()
		return false, nil
	case "exit", "quit", "q":
		return true, nil
	case "put":
		key, value, err := parseKeyValueArguments(parts)
		if err != nil {
			return false, err
		}
		err = db.Put(key, value)
		return false, err
	case "get":
		if len(parts) < 2 || len(parts) == 3 { // to little arguments or impossible 3
			return false, errors.New("invalid arguments")
		}
		value, err := db.Get(parts[1]) // get the value from database
		if err != nil {
			return false, err
		}
		if len(parts) == 2 { // there are no optional arguments
			fmt.Println(string(value))
			return false, nil
		} else {
			var f *os.File
			if len(parts) == 5 { // there should be an append flag
				if isFlag(parts[2], "d") && isFlag(parts[4], "a") {
					f, err = os.OpenFile(parts[3], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
				} else if isFlag(parts[3], "d") && isFlag(parts[2], "a") {
					f, err = os.OpenFile(parts[4], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
				} else { // flags are not present
					return false, errors.New("invalid arguments")
				}
			} else { // there is no append flag
				if isFlag(parts[2], "d") {
					f, err = os.OpenFile(parts[3], os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
				} else {
					return false, errors.New("invalid arguments")
				}
			}
			if err != nil {
				return false, err
			}
			defer func(f *os.File) {
				_ = f.Close()
			}(f)
			_, err = f.Write(value) // writing the value with the desired mode
			return false, err
		}
	case "delete":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		err := db.Delete(parts[1])
		return false, err
	case "rangescan":
		if len(parts) < 3 {
			return false, errors.New("invalid arguments")
		}
		var pageNumber uint64 = 1
		var pageSize uint64 = defaultPageSize
		var err error
		if len(parts) > 3 {
			pageNumber, err = strconv.ParseUint(parts[3], 10, 32)
			if err != nil {
				return false, err
			}
		}
		if len(parts) > 4 {
			pageSize, err = strconv.ParseUint(parts[4], 10, 32)
			if err != nil {
				return false, err
			}
		}
		records, err := db.RangeScan(parts[1], parts[2], int(pageNumber), int(pageSize))
		if err != nil {
			return false, err
		}
		fmt.Printf("Page %d, page size %d\n", pageNumber, pageSize)
		fmt.Println("(K - key, V - value)")
		for i, record := range records {
			if len(record.Value) > maxScanValueLength {
				fmt.Printf("  Record %d - K: %s  V: %s...\n", i, string(record.Key), string(record.Value[:maxScanValueLength]))
			} else {
				fmt.Printf("  Record %d - K: %s  V: %s\n", i, string(record.Key), string(record.Value))
			}
		}
		return false, nil
	case "prefixscan":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		var pageNumber uint64 = 1
		var pageSize uint64 = defaultPageSize
		var err error
		if len(parts) > 2 {
			pageNumber, err = strconv.ParseUint(parts[2], 10, 32)
			if err != nil {
				return false, err
			}
		}
		if len(parts) > 3 {
			pageSize, err = strconv.ParseUint(parts[3], 10, 32)
			if err != nil {
				return false, err
			}
		}
		records, err := db.PrefixScan(parts[1], int(pageNumber), int(pageSize))
		if err != nil {
			return false, err
		}
		fmt.Printf("Page %d, page size %d\n", pageNumber, pageSize)
		fmt.Println("(K - key, V - value)")
		for i, record := range records {
			if len(record.Value) > maxScanValueLength {
				fmt.Printf("  Record %d - K: %s  V: %s...\n", i, string(record.Key), string(record.Value[:maxScanValueLength]))
			} else {
				fmt.Printf("  Record %d - K: %s  V: %s\n", i, string(record.Key), string(record.Value))
			}
		}
		return false, nil
	case "newbf":
		if len(parts) < 4 {
			return false, errors.New("invalid arguments")
		}
		n, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return false, err
		}
		p, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return false, err
		}
		err = db.NewBF(parts[1], uint(n), p)
		return false, err
	case "deletebf":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		err := db.DeleteBF(parts[1])
		return false, err
	case "bfadd":
		key, value, err := parseKeyValueArguments(parts)
		if err != nil {
			return false, err
		}
		err = db.BFAdd(key, value)
		return false, err
	case "bfhaskey":
		key, value, err := parseKeyValueArguments(parts)
		if err != nil {
			return false, err
		}
		result, err := db.BFHasKey(key, value)
		if err != nil {
			return false, err
		}
		fmt.Println(result)
		return false, nil
	case "newcms":
		if len(parts) < 4 {
			return false, errors.New("invalid arguments")
		}
		epsilon, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return false, err
		}
		delta, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return false, err
		}
		err = db.NewCMS(parts[1], epsilon, delta)
		return false, err
	case "deletecms":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		err := db.DeleteCMS(parts[1])
		return false, err
	case "cmsadd":
		key, value, err := parseKeyValueArguments(parts)
		if err != nil {
			return false, err
		}
		err = db.CMSAdd(key, value)
		return false, err
	case "cmsget":
		key, value, err := parseKeyValueArguments(parts)
		if err != nil {
			return false, err
		}
		result, err := db.CMSGet(key, value)
		if err != nil {
			return false, err
		}
		fmt.Println(result)
		return false, nil
	case "newhll":
		if len(parts) < 3 {
			return false, errors.New("invalid arguments")
		}
		p, err := strconv.Atoi(parts[2])
		if err != nil {
			return false, err
		}
		err = db.NewHLL(parts[1], uint32(p))
		return false, err
	case "deletehll":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		err := db.DeleteHLL(parts[1])
		return false, err
	case "hlladd":
		key, value, err := parseKeyValueArguments(parts)
		if err != nil {
			return false, err
		}
		err = db.HLLAdd(key, value)
		return false, err
	case "hllestimate":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		result, err := db.HLLEstimate(parts[1])
		if err != nil {
			return false, err
		}
		fmt.Println(result)
		return false, nil
	case "shaddfingerprint":
		if len(parts) < 3 {
			return false, errors.New("invalid arguments")
		}
		err := db.SHAddFingerprint(parts[1], parts[2])
		return false, err
	case "shdeletefingerprint":
		if len(parts) < 2 {
			return false, errors.New("invalid arguments")
		}
		err := db.SHDeleteFingerprint(parts[1])
		return false, err
	case "shgethammingdistance":
		if len(parts) < 3 {
			return false, errors.New("invalid arguments")
		}
		result, err := db.SHGetHammingDistance(parts[1], parts[2])
		if err != nil {
			return false, err
		}
		fmt.Println(result)
		return false, nil
	default:
		return false, errors.New("invalid command")
	}
}

// parseKeyValueArguments reads key and value arguments from separated input for format: key <value | -s valueSourceFile>
func parseKeyValueArguments(parts []string) (string, []byte, error) {
	if len(parts) < 3 {
		return "", nil, errors.New("invalid arguments")
	} else if len(parts) == 3 {
		return parts[1], []byte(parts[2]), nil
	} else {
		if !isFlag(parts[2], "s") {
			return "", nil, errors.New("invalid arguments")
		}
		value, err := readFile(parts[3])
		return parts[1], value, err
	}
}

// isFlag checks if the argument is a flag of the given character.
func isFlag(argument string, flag string) bool {
	return strings.ToLower(argument) == "-"+strings.ToLower(flag) ||
		strings.ToLower(argument) == "/"+strings.ToLower(flag)
}

// readFile reads all bytes from a file and returns them.
func readFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	fStat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fSize := fStat.Size()
	result := make([]byte, fSize)
	_, err = f.Read(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// help prints all commands.
func help() {
	fmt.Println("General:")
	fmt.Println("  PUT key <value | -s valueSourceFile>")
	fmt.Println("  GET key [-d destinationFile [-a(append)]]")
	fmt.Println("  DELETE key")
	fmt.Println("  HELP | ? | COMMANDS")
	fmt.Println("  EXIT | QUIT | Q")
	fmt.Println()
	fmt.Println("Scans:")
	fmt.Println("  RangeScan startKey(inclusive) endKey(inclusive) [pageNumber] [pageSize]")
	fmt.Println("  PrefixScan prefix [pageNumber] [pageSize]")
	fmt.Println()
	fmt.Println("Bloom Filter:")
	fmt.Println("  NewBF key n(number of elements) p(false-positive probability)")
	fmt.Println("  DeleteBF key")
	fmt.Println("  BFAdd key <value | -s valueSourceFile>")
	fmt.Println("  BFHasKey key <value | -s valueSourceFile>")
	fmt.Println()
	fmt.Println("Count Min Sketch:")
	fmt.Println("  NewCMS key epsilon delta")
	fmt.Println("  DeleteCMS key")
	fmt.Println("  CMSAdd key <value | -s valueSourceFile>")
	fmt.Println("  CMSGet key <value | -s valueSourceFile>")
	fmt.Println()
	fmt.Println("HyperLogLog:")
	fmt.Println("  NewHLL key p(precision)")
	fmt.Println("  DeleteHLL key")
	fmt.Println("  HLLAdd key <value | -s valueSourceFile>")
	fmt.Println("  HLLEstimate key")
	fmt.Println()
	fmt.Println("SimHash:")
	fmt.Println("  SHAddFingerprint key text")
	fmt.Println("  SHDeleteFingerprint key")
	fmt.Println("  SHGetHammingDistance key1 key2")
}
