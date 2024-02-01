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

func parseAndExecute(input string, db *KeyValueStore) (bool, error) {
	r := regexp.MustCompile(`[^\s"']+|"([^"]*)"|'([^']*)'`)
	parts := r.FindAllString(input, -1)
	if parts == nil {
		return false, errors.New("invalid command")
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
					f, err = os.OpenFile(parts[3], os.O_APPEND|os.O_CREATE, 0644)
				} else if isFlag(parts[3], "d") && isFlag(parts[2], "a") {
					f, err = os.OpenFile(parts[4], os.O_APPEND|os.O_CREATE, 0644)
				} else { // flags are not present
					return false, errors.New("invalid arguments")
				}
			} else { // there is no append flag
				if isFlag(parts[2], "d") {
					f, err = os.OpenFile(parts[3], os.O_TRUNC|os.O_CREATE, 0644)
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
		return false, errors.New("not implemented")
	case "prefixscan":
		return false, errors.New("not implemented")
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

func isFlag(argument string, flag string) bool {
	return strings.ToLower(argument) == "-"+strings.ToLower(flag) ||
		strings.ToLower(argument) == "/"+strings.ToLower(flag)
}

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

func help() {
	fmt.Println("General:")
	fmt.Println("  PUT key <value | -s valueSourceFile>")
	fmt.Println("  GET key [-d destinationFile [-a(append)]]")
	fmt.Println("  DELETE key")
	fmt.Println("  HELP | ? | COMMANDS")
	fmt.Println("  EXIT | QUIT | Q")
	fmt.Println()
	fmt.Println("Scans:")
	fmt.Println("  RangeScan startKey(inclusive) endKey(inclusive)")
	fmt.Println("  PrefixScan prefix")
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
