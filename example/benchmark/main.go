/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
	"fmt"
	"os"

	"github.com/zjykzk/rocketmq-client-go/log"
)

type command interface {
	usage()
	run(args []string)
}

var (
	cmds = map[string]command{}
)

func registerCommand(name string, cmd command) {
	if cmd == nil {
		panic("empty command")
	}

	_, ok := cmds[name]
	if ok {
		panic(fmt.Sprintf("%s command existed", name))
	}

	cmds[name] = cmd
}

func usage() {
	println(os.Args[0] + " commandName [...]")
	for _, cmd := range cmds {
		cmd.usage()
	}
}

// go run *.go [command name] [command args]
// example:
// go run consumer -f TAG -m 50  -n 10.200.20.25:9988 -p -t 0%default
// go run producer -f TAG -m 50  -n 10.200.20.25:9988 -p -t 0%default -s 128 -i 128
func main() {
	if len(os.Args) < 2 {
		println("error:lack cmd name\n")
		usage()
		return
	}

	name := os.Args[1]
	cmd, ok := cmds[name]
	if !ok {
		fmt.Printf("command %s is not supported\n", name)
		usage()
		return
	}

	cmd.run(os.Args[2:])
}

func newLogger(filename string) (log.Logger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		println("create file error", err.Error())
		return nil, err
	}

	logger := log.New(file, "", log.Ldefault)
	logger.Level = log.Ldebug

	return logger, nil
}
