package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {

	filePath := os.Args[1]
	readFile, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileLines []string

	for fileScanner.Scan() {
		fileLines = append(fileLines, fileScanner.Text())
	}
	readFile.Close()

	for _, line := range fileLines {
		fmt.Println(reverse(line))
	}
}

func reverse(str string) string {
	bytes := []rune(str)
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}
	return string(bytes)
}
