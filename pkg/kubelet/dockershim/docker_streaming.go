/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dockershim

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os/exec"
	"strings"
	"time"
	"net"
	"net/http"

	dockertypes "github.com/docker/docker/api/types"

	"github.com/golang/glog"

	"k8s.io/client-go/tools/remotecommand"
	runtimeapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/v1alpha1/runtime"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/server/streaming"
	"k8s.io/kubernetes/pkg/kubelet/util/ioutils"

	"k8s.io/kubernetes/pkg/kubelet/dockershim/libdocker"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/api/core/v1"
	"syscall"
	"bufio"
)

type streamingRuntime struct {
	client      libdocker.Interface
	execHandler ExecHandler
}

var _ streaming.Runtime = &streamingRuntime{}

func (r *streamingRuntime) Exec(containerID string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	return r.exec(containerID, cmd, in, out, err, tty, resize, 0)
}

// Internal version of Exec adds a timeout.
func (r *streamingRuntime) exec(containerID string, cmd []string, in io.Reader, out, errw io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize, timeout time.Duration) error {
	container, err := checkContainerStatus(r.client, containerID)
	if err != nil {
		return err
	}
	return r.execHandler.ExecInContainer(r.client, container, cmd, in, out, errw, tty, resize, timeout)
}

func (r *streamingRuntime) Attach(containerID string, in io.Reader, out, errw io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	_, err := checkContainerStatus(r.client, containerID)
	if err != nil {
		return err
	}

	return attachContainer(r.client, containerID, in, out, errw, tty, resize)
}

func (r *streamingRuntime) PortForward(podSandboxID string, port int32, remoteForwarding bool, stream io.ReadWriteCloser, connection httpstream.Connection) error {
	if remoteForwarding {
		return remotePortForward(r.client, podSandboxID, port, stream, connection)
	} else {
		return localPortForward(r.client, podSandboxID, port, stream)
	}
}

// ExecSync executes a command in the container, and returns the stdout output.
// If command exits with a non-zero exit code, an error is returned.
func (ds *dockerService) ExecSync(containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	var stdoutBuffer, stderrBuffer bytes.Buffer
	err = ds.streamingRuntime.exec(containerID, cmd,
		nil, // in
		ioutils.WriteCloserWrapper(&stdoutBuffer),
		ioutils.WriteCloserWrapper(&stderrBuffer),
		false, // tty
		nil,   // resize
		timeout)
	return stdoutBuffer.Bytes(), stderrBuffer.Bytes(), err
}

// Exec prepares a streaming endpoint to execute a command in the container, and returns the address.
func (ds *dockerService) Exec(req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	if ds.streamingServer == nil {
		return nil, streaming.ErrorStreamingDisabled("exec")
	}
	_, err := checkContainerStatus(ds.client, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return ds.streamingServer.GetExec(req)
}

// Attach prepares a streaming endpoint to attach to a running container, and returns the address.
func (ds *dockerService) Attach(req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	if ds.streamingServer == nil {
		return nil, streaming.ErrorStreamingDisabled("attach")
	}
	_, err := checkContainerStatus(ds.client, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return ds.streamingServer.GetAttach(req)
}

// PortForward prepares a streaming endpoint to forward ports from a PodSandbox, and returns the address.
func (ds *dockerService) PortForward(req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	if ds.streamingServer == nil {
		return nil, streaming.ErrorStreamingDisabled("port forward")
	}
	_, err := checkContainerStatus(ds.client, req.PodSandboxId)
	if err != nil {
		return nil, err
	}
	// TODO(tallclair): Verify that ports are exposed.
	return ds.streamingServer.GetPortForward(req)
}

func checkContainerStatus(client libdocker.Interface, containerID string) (*dockertypes.ContainerJSON, error) {
	container, err := client.InspectContainer(containerID)
	if err != nil {
		return nil, err
	}
	if !container.State.Running {
		return nil, fmt.Errorf("container not running (%s)", container.ID)
	}
	return container, nil
}

func attachContainer(client libdocker.Interface, containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	// Have to start this before the call to client.AttachToContainer because client.AttachToContainer is a blocking
	// call :-( Otherwise, resize events don't get processed and the terminal never resizes.
	kubecontainer.HandleResizing(resize, func(size remotecommand.TerminalSize) {
		client.ResizeContainerTTY(containerID, uint(size.Height), uint(size.Width))
	})

	// TODO(random-liu): Do we really use the *Logs* field here?
	opts := dockertypes.ContainerAttachOptions{
		Stream: true,
		Stdin:  stdin != nil,
		Stdout: stdout != nil,
		Stderr: stderr != nil,
	}
	sopts := libdocker.StreamOptions{
		InputStream:  stdin,
		OutputStream: stdout,
		ErrorStream:  stderr,
		RawTerminal:  tty,
	}
	return client.AttachToContainer(containerID, opts, sopts)
}

// localPortForward handles port forwarding from user side to Pod side
func localPortForward(client libdocker.Interface, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	if port <= 0 || port > math.MaxUint16 {
		return fmt.Errorf("invalid port %d", port)
	}
	container, err := client.InspectContainer(podSandboxID)
	if err != nil {
		return err
	}

	if !container.State.Running {
		return fmt.Errorf("container not running (%s)", container.ID)
	}

	containerPid := container.State.Pid
	socatPath, lookupErr := exec.LookPath("socat")
	if lookupErr != nil {
		return fmt.Errorf("unable to do port forwarding: socat not found.")
	}

	args := []string{"-t", fmt.Sprintf("%d", containerPid), "-n", socatPath, "-", fmt.Sprintf("TCP4:localhost:%d", port)}

	nsenterPath, lookupErr := exec.LookPath("nsenter")
	if lookupErr != nil {
		return fmt.Errorf("unable to do port forwarding: nsenter not found.")
	}

	commandString := fmt.Sprintf("%s %s", nsenterPath, strings.Join(args, " "))
	glog.V(4).Infof("executing port forwarding command: %s", commandString)

	command := exec.Command(nsenterPath, args...)
	command.Stdout = stream

	stderr := new(bytes.Buffer)
	command.Stderr = stderr

	// If we use Stdin, command.Run() won't return until the goroutine that's copying
	// from stream finishes. Unfortunately, if you have a client like telnet connected
	// via port forwarding, as long as the user's telnet client is connected to the user's
	// local listener that port forwarding sets up, the telnet session never exits. This
	// means that even if socat has finished running, command.Run() won't ever return
	// (because the client still has the connection and stream open).
	//
	// The work around is to use StdinPipe(), as Wait() (called by Run()) closes the pipe
	// when the command (socat) exits.
	inPipe, err := command.StdinPipe()
	if err != nil {
		return fmt.Errorf("unable to do port forwarding: error creating stdin pipe: %v", err)
	}
	go func() {
		//io.Copy(inPipe, bytes.NewReader([]byte("test writing to socat")))
		io.Copy(inPipe, stream)
		inPipe.Close()
	}()

	if err := command.Run(); err != nil {
		return fmt.Errorf("%v: %s", err, stderr.String())
	}

	return nil
}

// remotePortForward handles port forwarding from Pod side to user side
func remotePortForward(client libdocker.Interface, podSandboxID string, port int32, stream io.ReadWriteCloser, connection httpstream.Connection) error {
	// port == 0 is allowed to indicate that stream is a command stream
	if port < 0 || port > math.MaxUint16 {
		return fmt.Errorf("invalid port %d", port)
	}
	container, err := client.InspectContainer(podSandboxID)
	if err != nil {
		return err
	}

	if !container.State.Running {
		return fmt.Errorf("container not running (%s)", container.ID)
	}

	containerPid := container.State.Pid

	// Listen on Unix domain socket for incoming connections from Pod
	unixDomainSocketPath := fmt.Sprintf("/tmp/kubelet-remote-forward-%d-%d.sock", containerPid, port)

	listener, err := listenOnUnixDomainSocket(unixDomainSocketPath)
	if err != nil {
		return err
	}

	unixDomainSocketListenerDone := make(chan interface{})
	go func() {
		defer close(unixDomainSocketListenerDone)
		if err := waitForConnection(listener, port, stream, connection); err != nil {
			runtime.HandleError(fmt.Errorf("error to listen on unix domain socket: %v", err))
		}
	}()

	podPortForwarderDone := make(chan interface{})
	podPortForwarderStopChan := make(chan interface{})
	go func() {
		defer close(podPortForwarderDone)
		if err := runPodPortForwarder(containerPid, port, unixDomainSocketPath, podPortForwarderStopChan); err != nil {
			runtime.HandleError(fmt.Errorf("error running socat as pod port forwarder: %v", err))
		}
	}()

	// Monitoring the control stream.
	// Stop all listeners for this port forwarding when remote write end of the stream is shutdown.
	go func() {
		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			text := scanner.Text()
			glog.V(4).Infof("Received %v", text)
		}
		if err := scanner.Err(); err != nil {
			runtime.HandleError(fmt.Errorf("error reading the control stream for pod port forwarder: %v", err))
		}
		close(podPortForwarderStopChan)
	}()

	// Waiting for the pod port forwarder to stop
	<-podPortForwarderDone
	glog.V(4).Infof("yuxzhu: socat stopped for remote port forward: %d", port)
	// stop the unix domain socket listener
	listener.Close()
	// Waiting for the unix domain socket listener to stop
	<-unixDomainSocketListenerDone
	glog.V(4).Infof("yuxzhu: Unix domain socket listener stopped for remote port forward: %d", port)
	return nil
}

// Run socat in the pod, listens on specified port, and forwards new connections to specified unix Domain socket
func runPodPortForwarder(containerPid int, port int32, unixDomainSocketPath string, stopChan <-chan interface{}) error {
	socatPath, lookupErr := exec.LookPath("socat")
	if lookupErr != nil {
		return fmt.Errorf("unable to do remote port forwarding: socat not found.")
	}

	args := []string{"-t", fmt.Sprintf("%d", containerPid), "-n", "-u", "-p", socatPath,
		fmt.Sprintf("TCP4-LISTEN:%d,reuseaddr,fork", port), "UNIX-CONNECT:" + unixDomainSocketPath}

	nsenterPath, lookupErr := exec.LookPath("nsenter")
	if lookupErr != nil {
		return fmt.Errorf("unable to do remote port forwarding: nsenter not found.")
	}

	commandString := fmt.Sprintf("%s %s", nsenterPath, strings.Join(args, " "))
	glog.V(4).Infof("executing remote port forwarding command: %s", commandString)

	command := exec.Command(nsenterPath, args...)
	command.Stdout = nil
	command.Stdin = nil
	stderr := new(bytes.Buffer)
	command.Stderr = stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return fmt.Errorf("%v: %s", err, stderr.String())
	}

	errChan := make(chan error)
	go func() {
		if err := command.Wait(); err != nil {
			errChan <- fmt.Errorf("%v: %s", err, stderr.String())
		}
		close(errChan)
	}()

	// waiting for stopChan
	select {
	case <-stopChan:
		// try to stop socat
		glog.V(4).Infof("Stopping socat for remote port forwarding: %v", port)
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil {
			glog.Warningf("Unable to stop socat: %v: %s", err, stderr.String())
			glog.V(4).Infof("Killing socat for remote port forwarding: %v", port)
			if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
				return fmt.Errorf("%v: %s", err, stderr.String())
			}
		}
		glog.V(4).Infof("Stopped socat for remote port forwarding: %v", port)
		return nil
	case err := <-errChan:
		return err
	}
}

func listenOnUnixDomainSocket(unixDomainSocketPath string) (net.Listener, error) {
	glog.V(4).Infof("Starting listening on Unix Domain Socket %s", unixDomainSocketPath)
	listener, err := net.Listen("unix", unixDomainSocketPath)
	if err != nil {
		return nil, fmt.Errorf("unable to listen on a Unix domain socket for remote port forwarding: %s", err)
	}
	glog.V(4).Infof("Listening on Unix Domain Socket %s", unixDomainSocketPath)
	return listener, nil
}

func waitForConnection(listener net.Listener, port int32, stream io.ReadWriteCloser, connection httpstream.Connection) error {
	for {
		connFromPod, err := listener.Accept()
		glog.V(4).Infof("Incoming connection from Pod side")
		if err != nil {
			return fmt.Errorf("unable to accept a connection from Pod port %d: %v", port, err)
		}
		// create error stream
		headers := http.Header{}
		headers.Set(v1.StreamType, v1.StreamTypeData)
		headers.Set(v1.PortHeader, fmt.Sprintf("%d", port))
		//headers.Set(v1.PortForwardRequestIDHeader, strconv.Itoa(requestID))
		glog.V(4).Infof("yuxzhu: creating new stream")
		newStream, err := connection.CreateStream(headers)
		if err != nil {
			stream.Write([]byte(fmt.Sprintf("NEW STREAM Error")))
			glog.V(4).Infof("yuxzhu: Unable to create a new stream for the new connection")
			break
		}
		glog.V(4).Infof("yuxzhu: Created a new stream for the new connection")
		go handleRemotePortForwardingStream(port, newStream, connFromPod)
	}
	return nil
}

func handleRemotePortForwardingStream(port int32, stream io.ReadWriteCloser, connFromPod io.ReadWriteCloser) {
	defer stream.Close()
	defer connFromPod.Close()
	readChan := make(chan interface{})
	writeChan := make(chan interface{})
	go func() {
		// Copy from pod connection to stream
		if _, err := io.Copy(stream, connFromPod); err != nil {
			glog.V(4).Infof("yuxzhu: error copying from pod connection to stream: %v", err)
		}
		glog.V(4).Infof("yuxzhu: done copying from pod connection to stream: %v", port)
		close(writeChan)
	}()
	go func() {
		// Copy from stream to pod connection
		if _, err := io.Copy(connFromPod, stream); err != nil {
			glog.V(4).Infof("yuxzhu: error copying from stream to pod connection: %v", err)
		}
		glog.V(4).Infof("yuxzhu: done copying from stream to pod connection: %v", port)
		close(readChan)
	}()
	select {
	case <-readChan:
	case <-writeChan:
	}
	glog.V(4).Infof("yuxzhu: connection lost from pod port: %v", port)
}
