/*
Copyright 2015 The Kubernetes Authors.

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

package portforward

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/runtime"
	"bufio"
	"github.com/golang/glog"
	"math"
)

// TODO move to API machinery and re-unify with kubelet/server/portfoward
// The subprotocol "portforward.k8s.io" is used for port forwarding.
const PortForwardProtocolV1Name = "portforward.k8s.io"

// PortForwarder knows how to listen for local connections and forward them to
// a remote pod via an upgraded HTTP request.
type PortForwarder struct {
	ports    []ForwardedPort
	remote   bool
	stopChan <-chan struct{}

	dialer        httpstream.Dialer
	streamConn    httpstream.Connection
	newStreamChan <-chan httpstream.Stream
	listeners     []io.Closer
	Ready         chan struct{}
	requestIDLock sync.Mutex
	requestID     int
	out           io.Writer
	errOut        io.Writer
}

// ForwardedPort contains a Local:Remote port pairing.
type ForwardedPort struct {
	Local  uint16
	Remote uint16
}

/*
	valid port specifications:

	5000
	- forwards from localhost:5000 to pod:5000

	8888:5000
	- forwards from localhost:8888 to pod:5000

	0:5000
	:5000
	- selects a random available local port,
	  forwards from localhost:<random port> to pod:5000
*/
func parsePorts(ports []string) ([]ForwardedPort, error) {
	var forwards []ForwardedPort
	for _, portString := range ports {
		parts := strings.Split(portString, ":")
		var localString, remoteString string
		if len(parts) == 1 {
			localString = parts[0]
			remoteString = parts[0]
		} else if len(parts) == 2 {
			localString = parts[0]
			if localString == "" {
				// support :5000
				localString = "0"
			}
			remoteString = parts[1]
		} else {
			return nil, fmt.Errorf("Invalid port format '%s'", portString)
		}

		localPort, err := strconv.ParseUint(localString, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("Error parsing local port '%s': %s", localString, err)
		}

		remotePort, err := strconv.ParseUint(remoteString, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("Error parsing remote port '%s': %s", remoteString, err)
		}
		if remotePort == 0 {
			return nil, fmt.Errorf("Remote port must be > 0")
		}

		forwards = append(forwards, ForwardedPort{uint16(localPort), uint16(remotePort)})
	}

	return forwards, nil
}

// New creates a new PortForwarder.
func New(dialer httpstream.Dialer, newStreamChan <-chan httpstream.Stream, ports []string, remote bool, stopChan <-chan struct{}, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	if len(ports) == 0 {
		return nil, errors.New("You must specify at least 1 port")
	}
	parsedPorts, err := parsePorts(ports)
	if err != nil {
		return nil, err
	}
	return &PortForwarder{
		dialer:        dialer,
		newStreamChan: newStreamChan,
		ports:         parsedPorts,
		remote:        remote,
		stopChan:      stopChan,
		Ready:         readyChan,
		out:           out,
		errOut:        errOut,
	}, nil
}

// ForwardPorts formats and executes a port forwarding request. The connection will remain
// open until stopChan is closed.
func (pf *PortForwarder) ForwardPorts() error {
	defer pf.Close()

	var err error
	pf.streamConn, _, err = pf.dialer.Dial(PortForwardProtocolV1Name)
	if err != nil {
		return fmt.Errorf("error upgrading connection: %s", err)
	}
	defer pf.streamConn.Close()
	if !pf.remote {
		return pf.forward()
	} else {
		return pf.remoteForward()
	}
	return pf.forward()
}

// forward dials the remote host specific in req, upgrades the request, starts
// listeners for each port specified in ports, and forwards local connections
// to the remote host via streams.
func (pf *PortForwarder) forward() error {
	var err error

	listenSuccess := false
	for _, port := range pf.ports {
		err = pf.listenOnPort(&port)
		switch {
		case err == nil:
			listenSuccess = true
		default:
			if pf.errOut != nil {
				fmt.Fprintf(pf.errOut, "Unable to listen on port %d: %v\n", port.Local, err)
			}
		}
	}

	if !listenSuccess {
		return fmt.Errorf("Unable to listen on any of the requested ports: %v", pf.ports)
	}

	if pf.Ready != nil {
		close(pf.Ready)
	}
	glog.Infof("yuxzhu: doing local port forwarding...")
	// wait for interrupt or conn closure
	select {
	case <-pf.stopChan:
		glog.Infof("yuxzhu: stopChan received for local port forwarding...")
	case <-pf.streamConn.CloseChan():
		glog.Infof("yuxzhu: streamConn.CloseChan() received local port forwarding...")
		runtime.HandleError(errors.New("lost connection to pod"))
	}
	glog.Infof("yuxzhu: localForward stopped")
	return nil
}

// remoteForward dials the remote host specific in req, upgrades the request, starts
// remote listeners for each port specified in ports, and forwards remote connections
// to the local host via streams.
func (pf *PortForwarder) remoteForward() error {
	var err error

	go pf.waitForRemoteConnections()

	remoteListenSuccess := false
	for _, port := range pf.ports {
		glog.Infof("yuxzhu: will listen on remote port %d", port.Remote)
		err = pf.listenOnRemotePort(&port)
		switch {
		case err == nil:
			remoteListenSuccess = true
		default:
			if pf.errOut != nil {
				fmt.Fprintf(pf.errOut, "Unable to listen on port %d in the pod: %v\n", port.Remote, err)
			}
		}
	}
	if !remoteListenSuccess {
		return fmt.Errorf("Unable to listen on any of the requested Pod ports")
	}
	glog.Infof("yuxzhu: doing remote port forwarding...")

	// wait for interrupt or conn closure
	select {
	case <-pf.stopChan:
		glog.Infof("yuxzhu: stopChan received for remote port forwarding...")
	case <-pf.streamConn.CloseChan():
		glog.Infof("yuxzhu: streamConn.CloseChan() received remote port forwarding...")
		runtime.HandleError(errors.New("lost connection to pod"))
	}
	glog.Infof("yuxzhu: remoteForward stopped")
	return nil
}

// listenOnPort delegates tcp4 and tcp6 listener creation and waits for connections on both of these addresses.
// If both listener creation fail, an error is raised.
func (pf *PortForwarder) listenOnPort(port *ForwardedPort) error {
	errTcp4 := pf.listenOnPortAndAddress(port, "tcp4", "127.0.0.1")
	errTcp6 := pf.listenOnPortAndAddress(port, "tcp6", "::1")
	if errTcp6 != nil {
		return fmt.Errorf("TCP6 listen failed %s", errTcp6)
	}
	if errTcp4 != nil && errTcp6 != nil {
		return fmt.Errorf("All listeners failed to create with the following errors: %s, %s", errTcp4, errTcp6)
	}
	return nil
}

// remoteListenOnPort asks the remote server to listen on specified port in the pod.
// A pair of streams, aka listener stream pair, will be created for communication between kubectl and the remote listener.
// The error listener stream is used to get error message from the remote server if the remote listeners fail to start, like what local port forwarding does.
// The data stream is used to transfer the actual remote hostname and port if the remote listeners start.
// If the data stream closes, the remote server should stop the listeners in the pod.
func (pf *PortForwarder) listenOnRemotePort(port *ForwardedPort) error {
	requestID := pf.nextRequestID()

	// create error stream
	headers := http.Header{}
	headers.Set(v1.StreamType, v1.StreamTypeError)
	headers.Set(v1.PortForwardRemoteHeader, "1")
	headers.Set(v1.PortHeader, strconv.Itoa(int(port.Remote)))
	headers.Set(v1.PortForwardRequestIDHeader, strconv.Itoa(requestID))
	errorStream, err := pf.streamConn.CreateStream(headers)
	if err != nil {
		runtime.HandleError(fmt.Errorf("error creating listener error stream for remote port forwarding %d -> %d: %v", port.Remote, port.Local, err))
		return err
	}
	// we're not writing to error control stream
	errorStream.Close()

	// create data stream
	headers.Set(v1.StreamType, v1.StreamTypeData)
	dataStream, err := pf.streamConn.CreateStream(headers)
	if err != nil {
		return fmt.Errorf("error creating remote forwarding stream for port %d -> %d: %v", port.Remote, port.Local, err)
	}
	//pf.listeners = append(pf.listeners, dataStream)

	go pf.handleControlStreams(port, dataStream, errorStream)

	return nil
}

func (pf *PortForwarder) handleControlStreams(port *ForwardedPort, dataStream, errorStream httpstream.Stream) {
	defer dataStream.Close()
	errorChan := make(chan error)
	go func() {
		defer close(errorChan)
		message, err := ioutil.ReadAll(errorStream)
		switch {
		case err != nil:
			errorChan <- fmt.Errorf("error reading from listener error stream for remote port forwarding %d -> %d: %v", port.Remote, port.Local, err)
		case len(message) > 0:
			errorChan <- fmt.Errorf("an error occurred forwarding remote port %d -> %d: %v", port.Remote, port.Local, string(message))
		}
	}()

	dataStreamDone := make(chan struct{})
	go func() {
		defer close(dataStreamDone)
		scanner := bufio.NewScanner(dataStream)
		// receive incoming commands sent by kubelet
		for scanner.Scan() {
			line := scanner.Text()
			if pf.out != nil {
				fmt.Fprintf(pf.out, "Received from remote: %v", line)
			}
		}
		if err := scanner.Err(); err != nil {
			runtime.HandleError(fmt.Errorf("error reading from command data stream for remote port %d -> %d: %v", port.Remote, port.Local, err))
		}
	}()

	// wait for the data control stream to be closed or for all listeners to finish
	<-dataStreamDone

	// always expect something on errorChan (it may be nil)
	err := <-errorChan
	if err != nil {
		runtime.HandleError(err)
	}
}

// listenOnPortAndAddress delegates listener creation and waits for new connections
// in the background f
func (pf *PortForwarder) listenOnPortAndAddress(port *ForwardedPort, protocol string, address string) error {
	listener, err := pf.getListener(protocol, address, port)
	if err != nil {
		return err
	}
	pf.listeners = append(pf.listeners, listener)
	go pf.waitForConnection(listener, *port)
	return nil
}

// getListener creates a listener on the interface targeted by the given hostname on the given port with
// the given protocol. protocol is in net.Listen style which basically admits values like tcp, tcp4, tcp6
func (pf *PortForwarder) getListener(protocol string, hostname string, port *ForwardedPort) (net.Listener, error) {
	listener, err := net.Listen(protocol, net.JoinHostPort(hostname, strconv.Itoa(int(port.Local))))
	if err != nil {
		return nil, fmt.Errorf("Unable to create listener: Error %s", err)
	}
	listenerAddress := listener.Addr().String()
	host, localPort, _ := net.SplitHostPort(listenerAddress)
	localPortUInt, err := strconv.ParseUint(localPort, 10, 16)

	if err != nil {
		return nil, fmt.Errorf("Error parsing local port: %s from %s (%s)", err, listenerAddress, host)
	}
	port.Local = uint16(localPortUInt)
	if pf.out != nil {
		fmt.Fprintf(pf.out, "Forwarding from %s:%d -> %d\n", hostname, localPortUInt, port.Remote)
	}

	return listener, nil
}

// waitForConnection waits for new connections to listener and handles them in
// the background.
func (pf *PortForwarder) waitForConnection(listener net.Listener, port ForwardedPort) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			// TODO consider using something like https://github.com/hydrogen18/stoppableListener?
			if !strings.Contains(strings.ToLower(err.Error()), "use of closed network connection") {
				runtime.HandleError(fmt.Errorf("Error accepting connection on port %d: %v", port.Local, err))
			}
			return
		}
		go pf.handleConnection(conn, port)
	}
}

// waitForRemoteConnections waits for new connections from the Pod and handles them in
// the background.
func (pf *PortForwarder) waitForRemoteConnections() error {
	for {
		glog.Info("yuxzhu: waiting from remote connections")
		select {
		case stream, ok := <-pf.newStreamChan:
			glog.Info("yuxzhu: incoming remote connection")
			if !ok {
				break
			}
			glog.Info("yuxzhu: will process incoming remote connection")
			go pf.handleRemoteConnection(stream)
		}
	}
	return nil
}

func (pf *PortForwarder) handleRemoteConnection(stream httpstream.Stream) {
	defer stream.Close()
	// make sure it has a valid port header
	portString := stream.Headers().Get(v1.PortHeader)
	if len(portString) == 0 {
		runtime.HandleError(fmt.Errorf("%q header is required", v1.PortHeader))
		return
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid local port number: %v", err))
		return
	}
	if port <= 0 || port > math.MaxUint16 {
		runtime.HandleError(fmt.Errorf("local port number is out of range: %d", port))
		return
	}
	// FIXME: assuming local port == pod port
	glog.Infof("connecting to local port %d.", port)
	conn, err := net.Dial("tcp4", net.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to connect to local port %d", port))
		return
	}
	defer conn.Close()
	readChan := make(chan interface{})
	writeChan := make(chan interface{})
	go func() {
		// Copy from the local port to the remote side.
		if _, err := io.Copy(stream, conn); err != nil {
			runtime.HandleError(fmt.Errorf("error copying from local connection to remote stream: %v", err))
		}
		glog.Infof("local -> pod %d closed", port)
		close(writeChan)
	}()
	go func() {
		// Copy from the remote side to the local port.
		if _, err := io.Copy(conn, stream); err != nil {
			runtime.HandleError(fmt.Errorf("error copying from remote stream to local connection: %v", err))
		}
		glog.Infof("pod %d->local closed", port)
		close(readChan)
	}()

	select {
	case <-readChan:
	case <-writeChan:
	}
	glog.Infof("connection lost for pod port forwarding %d.", port)
}

func (pf *PortForwarder) nextRequestID() int {
	pf.requestIDLock.Lock()
	defer pf.requestIDLock.Unlock()
	id := pf.requestID
	pf.requestID++
	return id
}

// handleConnection copies data between the local connection and the stream to
// the remote server.
func (pf *PortForwarder) handleConnection(conn net.Conn, port ForwardedPort) {
	defer conn.Close()

	if pf.out != nil {
		fmt.Fprintf(pf.out, "Handling connection for %d\n", port.Local)
	}

	requestID := pf.nextRequestID()

	// create error stream
	headers := http.Header{}
	headers.Set(v1.StreamType, v1.StreamTypeError)
	headers.Set(v1.PortHeader, fmt.Sprintf("%d", port.Remote))
	headers.Set(v1.PortForwardRequestIDHeader, strconv.Itoa(requestID))
	errorStream, err := pf.streamConn.CreateStream(headers)
	if err != nil {
		runtime.HandleError(fmt.Errorf("error creating error stream for port %d -> %d: %v", port.Local, port.Remote, err))
		return
	}
	// we're not writing to this stream
	errorStream.Close()

	errorChan := make(chan error)
	go func() {
		message, err := ioutil.ReadAll(errorStream)
		switch {
		case err != nil:
			errorChan <- fmt.Errorf("error reading from error stream for port %d -> %d: %v", port.Local, port.Remote, err)
		case len(message) > 0:
			errorChan <- fmt.Errorf("an error occurred forwarding %d -> %d: %v", port.Local, port.Remote, string(message))
		}
		close(errorChan)
	}()

	// create data stream
	headers.Set(v1.StreamType, v1.StreamTypeData)
	dataStream, err := pf.streamConn.CreateStream(headers)
	if err != nil {
		runtime.HandleError(fmt.Errorf("error creating forwarding stream for port %d -> %d: %v", port.Local, port.Remote, err))
		return
	}

	localError := make(chan struct{})
	remoteDone := make(chan struct{})

	go func() {
		// Copy from the remote side to the local port.
		if _, err := io.Copy(conn, dataStream); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			runtime.HandleError(fmt.Errorf("error copying from remote stream to local connection: %v", err))
		}

		// inform the select below that the remote copy is done
		close(remoteDone)
	}()

	go func() {
		// inform server we're not sending any more data after copy unblocks
		defer dataStream.Close()

		// Copy from the local port to the remote side.
		if _, err := io.Copy(dataStream, conn); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			runtime.HandleError(fmt.Errorf("error copying from local connection to remote stream: %v", err))
			// break out of the select below without waiting for the other copy to finish
			close(localError)
		}
	}()

	// wait for either a local->remote error or for copying from remote->local to finish
	select {
	case <-remoteDone:
	case <-localError:
	}

	// always expect something on errorChan (it may be nil)
	err = <-errorChan
	if err != nil {
		runtime.HandleError(err)
	}
}

func (pf *PortForwarder) Close() {
	// stop all listeners
	for _, l := range pf.listeners {
		if err := l.Close(); err != nil {
			runtime.HandleError(fmt.Errorf("error closing listener: %v", err))
		}
	}
}
