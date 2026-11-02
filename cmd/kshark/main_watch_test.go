//go:build unix

package main

import (
	"bufio"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain_WatchModeStopsOnSignal(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	props := deadBrokerProps(t, dir)

	oldArgs, oldFlags, oldExit, oldStdout, oldLogger := os.Args, flag.CommandLine, exit, os.Stdout, slog.Default()
	t.Cleanup(func() {
		os.Args, flag.CommandLine, exit, os.Stdout = oldArgs, oldFlags, oldExit, oldStdout
		slog.SetDefault(oldLogger)
	})
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Args = []string{"kshark", "-props", props, "-diag=false", "-kafka-timeout", "1s", "-watch", "1h", "-json", "reports/watch.json"}
	flag.CommandLine = flag.NewFlagSet("kshark", flag.ContinueOnError)
	os.Stdout = outW

	codes := make(chan int, 1)
	exit = func(code int) { panic(exitPanic(code)) }
	go func() {
		defer func() {
			if p := recover(); p != nil {
				codes <- int(p.(exitPanic))
				return
			}
			codes <- 0
		}()
		main()
	}()

	lines := make(chan string, 64)
	go func() {
		sc := bufio.NewScanner(outR)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()

	var seen []string
	deadline := time.After(60 * time.Second)
	for waiting := true; waiting; {
		select {
		case line := <-lines:
			seen = append(seen, line)
			if strings.Contains(line, "Next scan in") {
				waiting = false
			}
		case <-deadline:
			t.Fatalf("watch loop never reached the sleep; output: %v", seen)
		}
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	var code int
	select {
	case code = <-codes:
	case <-time.After(30 * time.Second):
		t.Fatal("watch mode did not stop after SIGINT")
	}
	outW.Close()
	for line := range lines {
		seen = append(seen, line)
	}
	out := strings.Join(seen, "\n")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 for the L4 failure", code)
	}
	for _, want := range []string{"kshark watch — iteration 1", "Watch mode stopped after iteration 1."} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "reports", "watch_001.json")); err != nil {
		t.Errorf("watch report missing: %v", err)
	}
}
