"""
Module implements the infrastructure that launches a JVM with
the Py4J Gateway Server JVM.
"""

import os
import shlex
import signal
import socket

import select
import struct

from py4j.java_gateway import JavaGateway, GatewayClient, java_import
from subprocess import Popen, PIPE
import eventstore.common

def start_gateway():
    """Start the JVM gateway."""
    if 'EVENT_HOME' not in os.environ and 'SPARK_HOME' not in os.environ:   
        raise OSError("SPARK_HOME environment variable is not present")

    # Start a server socket that receives the port number from the PythonEventGatewayServer
    callback_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    callback_socket.bind(('127.0.0.1', 0))      # assign it to any port
    callback_socket.listen(1)
    callback_socket_host, callback_socket_port = callback_socket.getsockname()

    script = "./bin/eventstore-class.sh"
    command = ["java"]
    if 'EVENT_HOME' in os.environ:
        # for docker shell backward compatibility
        command_args = ' '.join([
            "-cp {}/target/scala-2.11/ibm-event_2.11-1.0.jar:{}/jars/*".format(os.environ.get("EVENT_HOME"),os.environ.get("SPARK_HOME")),
            "com.ibm.event.api.python.PythonEventGatewayServer",
            "--callback-host={}".format(callback_socket_host),
            "--callback-port={}".format(callback_socket_port)
        ])
    else :
        # production env
        command_args = ' '.join([
            "-cp {}/jars/*".format(os.environ.get("SPARK_HOME")),
            "com.ibm.event.api.python.PythonEventGatewayServer",
            "--callback-host={}".format(callback_socket_host),
            "--callback-port={}".format(callback_socket_port)
        ])
    command = command + shlex.split(command_args)

    # Launch the Java gateway
    # Don't send SIGINT to the Java gateway
    def preexec_func():
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    proc = Popen(command, stdin=PIPE, preexec_fn=preexec_func(),
                 env=dict(os.environ))
    gateway_port = None

    while gateway_port is None and proc.poll() is None:
        wait_for = 1 # seconds
        readable, _, _ = select.select([callback_socket], [], [], wait_for)
        if callback_socket in readable:
            # Socket is present
            gateway_connection = callback_socket.accept()[0]
            # Create a file object "read binary" out of the socket connection and read 4 bytes as int
            gateway_port = _read_int(gateway_connection.makefile(mode="rb"))
            gateway_connection.close()
            callback_socket.close()
    if gateway_port is None:
        raise Exception("Java gateway process exited before sending the driver the port number.")

    # auto_convert=True instructs Py4J to automatically convert Python collections into Java collections
    gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)

    # Import Scala classes
    java_import(gateway.jvm, "org.apache.spark.sql.ibm.event.*")
    java_import(gateway.jvm, "com.ibm.event.sql.*")
    java_import(gateway.jvm, "com.ibm.event.catalog.*")
    java_import(gateway.jvm, "com.ibm.event.oltp.*")
    java_import(gateway.jvm, "com.ibm.event.common.*")
    return gateway


def _read_int(stream):
    the_int = stream.read(4)        # read as string object
    if not the_int:
        raise EOFError
    return struct.unpack("!i", the_int)[0]  # read big-endian int32
