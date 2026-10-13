package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/internal/service"
)

func run(args []string, errOut io.Writer) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return runReference(args, errOut)
	}
	if args[0] == "reference" {
		return runReference(args[1:], errOut)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	disarmed := make(chan struct{})
	go func() {
		<-ctx.Done()
		stop()
		close(disarmed)
	}()
	defer func() { stop(); <-disarmed }()
	return runServiceCommand(ctx, args, os.Stdin, errOut)
}

func runServiceCommand(ctx context.Context, args []string, input io.Reader, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("a service command is required: serve, init, admin create or admin reset")
	}
	command, arguments := args[0], args[1:]
	if command == "admin" {
		if len(arguments) == 0 {
			return errors.New("admin requires create or reset")
		}
		command += " " + arguments[0]
		arguments = arguments[1:]
	}
	flags := flag.NewFlagSet("remote-fs-server "+command, flag.ContinueOnError)
	flags.SetOutput(output)
	configPath := flags.String("config", "", "service YAML configuration file")
	login := flags.String("login", "", "local administrator login name")
	display := flags.String("display-name", "", "administrator display name")
	passwordPath := flags.String("password-file", "", "owner-only password file; otherwise read from the terminal")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errUsage
	}
	if flags.NArg() != 0 {
		return errors.New("service commands take flags, not positional arguments")
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}
	if command != "serve" && command != "init" && command != "admin create" && command != "admin reset" {
		return fmt.Errorf("unknown service command %q", command)
	}
	if command != "admin create" && command != "admin reset" && (*login != "" || *display != "" || *passwordPath != "") {
		return errors.New("administrator flags require admin create or admin reset")
	}
	config, err := service.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	switch command {
	case "serve":
		return serveService(ctx, config, output)
	case "init":
		if err := service.InitState(ctx, config); err != nil {
			return err
		}
		return serviceEvent(output, "state_initialized")
	case "admin create", "admin reset":
		if *login == "" || (command == "admin create" && *display == "") {
			return errors.New("admin create requires --login and --display-name; admin reset requires --login")
		}
		password, err := administratorPassword(ctx, input, output, *passwordPath)
		if err != nil {
			return err
		}
		return administratorCommand(ctx, config, command, *login, *display, password, output)
	}
	panic("validated service command has no implementation")
}

func administratorCommand(ctx context.Context, config service.Config, command, login, display, password string, output io.Writer) (returned error) {
	maintenance, err := service.OpenMaintenance(ctx, config)
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, maintenance.Close(context.WithoutCancel(ctx))) }()
	if command == "admin create" {
		_, err = maintenance.AddAdmin(ctx, login, display, password)
	} else {
		_, err = maintenance.ResetAdminPassword(ctx, login, password)
	}
	if err != nil {
		return err
	}
	return serviceEvent(output, "administrator_updated")
}

func serviceEvent(output io.Writer, event string) error {
	return json.NewEncoder(output).Encode(struct {
		Time  string `json:"time"`
		Level string `json:"level"`
		Event string `json:"event"`
	}{time.Now().UTC().Format(time.RFC3339Nano), "info", event})
}

func serveService(ctx context.Context, config service.Config, output io.Writer) error {
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		return fmt.Errorf("listen for the service: %w", err)
	}
	return withListener(listener, func() (returned error) {
		application, err := service.Open(ctx, config)
		if err != nil {
			return err
		}
		closing := false
		defer func() {
			if !closing {
				closeCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
				defer cancel()
				returned = errors.Join(returned, application.Close(closeCtx))
			}
		}()
		connections := newConnectionTracker()
		httpOptions := defaultStandaloneHTTPOptions()
		httpServer := &http.Server{
			Handler:           application,
			ReadHeaderTimeout: httpOptions.readHeaderTimeout,
			IdleTimeout:       httpOptions.idleTimeout,
			ErrorLog:          log.New(output, "remote-fs-server: ", log.LstdFlags),
			ConnState:         connections.update,
		}
		httpServer.RegisterOnShutdown(application.Stop)
		stopped := make(chan error, 1)
		go func() { stopped <- httpServer.Serve(listener) }()
		if err := serviceEvent(output, "serving"); err != nil {
			application.Stop()
			return errors.Join(err, connections.closeNew(), httpServer.Close(), expectedClose(<-stopped))
		}
		select {
		case err = <-stopped:
		case <-ctx.Done():
		}
		application.Stop()
		closeCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		connectionErr := connections.closeNew()
		shutdownErr := httpServer.Shutdown(closeCtx)
		var forceErr error
		if shutdownErr != nil {
			forceErr = httpServer.Close()
		}
		if err == nil {
			err = <-stopped
		}
		closing = true
		closeErr := application.Close(closeCtx)
		event := "stopped"
		if errors.Join(shutdownErr, forceErr, closeErr, connectionErr) != nil {
			event = "shutdown_failed"
		}
		eventErr := serviceEvent(output, event)
		return errors.Join(expectedClose(err), connectionErr, shutdownErr, forceErr, closeErr, eventErr)
	})
}
