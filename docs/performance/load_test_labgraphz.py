#!/usr/bin/env python3
# Copyright 2004-present Facebook. All Rights Reserved.

import time


from labgraph import Message, Node, Graph
from labgraph.graphs import Config, Node, Topic, background, subscriber
from labgraph.loggers.logger import get_logger
from labgraph.zmq_node.zmq_poller_node import ZMQPollerNode, ZMQPollerConfig
from labgraph.zmq_node.zmq_sender_node import ZMQSenderNode, ZMQSenderConfig
from labgraph.zmq_node import ZMQMessage
from labgraph.util.testing import get_free_port, get_test_filename
import zmq
from zmq.asyncio import Context, Poller
import asyncio
import numpy as np

ZMQ_ADDR = "tcp://127.0.0.1"
ZMQ_TOPIC = "zmq_topic"
MESSAGE_SIZE = 50000
SAMPLE_RATE = 2000

logger = get_logger(__name__)
context = Context.instance()


class ZMQSender(ZMQSenderNode):
    """
    Represents a node in a LabGraph graph that sends data to a ZMQ socket.

    Args:
        write_addr: The address to which ZMQ data should be written.
        zmq_topic: The ZMQ topic being sent.
    """
    def setup(self) -> None:
        self.context = context
        self.socket = self.context.socket(zmq.PUSH)
        self.monitor = self.socket.get_monitor_socket()
        logger.debug(f"{self}:binding to {self.config.write_addr}")
        self.socket.bind(self.config.write_addr)
        self.topic = self.config.zmq_topic
        self.has_subscribers = False

    def cleanup(self) -> None:
        self.socket.close()

    @background
    async def _socket_monitor(self) -> None:
        while True:
            monitor_result = await self.monitor.poll(100, zmq.POLLIN)
            if monitor_result:
                data = await self.monitor.recv_multipart()
                evt = parse_monitor_message(data)

                event = ZMQEvent(evt["event"])
                logger.debug(f"{self}:{event.name}")

                if event == ZMQEvent.EVENT_ACCEPTED:
                    logger.debug(f"{self}:subscriber joined")
                    self.has_subscribers = True
                elif event in (
                    ZMQEvent.EVENT_DISCONNECTED,
                    ZMQEvent.EVENT_MONITOR_STOPPED,
                    ZMQEvent.EVENT_CLOSED,
                ):
                    break

    async def publish(self) -> None:
        for _ in range(MESSAGE_SIZE):
            await self.socket.send_multipart([str(time.perf_counter()).encode('ascii')])
            await asyncio.sleep(1 / SAMPLE_RATE)


class ZMQPoller(ZMQPollerNode):
    """
    Represents a node in the graph which polls data from ZMQ.
    Data polled from ZMQ.

    Args:
        read_addr: The address from which ZMQ data should be polled.
        zmq_topic: The ZMQ topic being polled.
        timeout:
            The maximum amount of time (in seconds) that should be
            spent polling a ZMQ socket each time.  Defaults to
            FOREVER_POLL_TIME if not specified.
        exit_condition:
            An optional ZMQ event code specifying the event which,
            if encountered by the monitor, should signal the termination
            of this particular node's activity.
    """
    # List of latency. Latency is defined as the duration from message sent to message received.
    latency = []

    def setup(self) -> None:
        self.context = context
        self.socket = self.context.socket(zmq.PULL)
        self.monitor = self.socket.get_monitor_socket()
        self.socket.connect(self.config.read_addr)
        self.topic = self.config.zmq_topic
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.socket_open = False

    def cleanup(self) -> None:
        self.socket.close()

    @background
    async def socket_monitor(self) -> None:
        while True:
            monitor_result = await self.monitor.poll(100, zmq.POLLIN)
            if monitor_result:
                data = await self.monitor.recv_multipart()
                evt = parse_monitor_message(data)

                event = ZMQEvent(evt["event"])
                logger.debug(f"{self}:{event.name}")

                if event == ZMQEvent.EVENT_CONNECTED:
                    self.socket_open = True
                elif event == ZMQEvent.EVENT_CLOSED:
                    was_open = self.socket_open
                    self.socket_open = False
                    # ZMQ seems to be sending spurious CLOSED event when we
                    # try to connect before the source is running. Only give up
                    # if we were previously connected. If we give up now, we
                    # will never unblock zmq_publisher.
                    if was_open:
                        break
                elif event in (
                    ZMQEvent.EVENT_DISCONNECTED,
                    ZMQEvent.EVENT_MONITOR_STOPPED,
                ):
                    self.socket_open = False
                    break

    async def receive(self) -> None:
        for i in range(MESSAGE_SIZE):
            events = await self.poller.poll()
            if self.socket in dict(events):
                msg = await self.socket.recv_multipart()
                # Convert latency from second to millisecond and append to latency list.
                self.latency.append((time.perf_counter() - float(msg[0])) * 1000)


def test_labgraphz() -> None:
    """
    Tests the Latency performance of LabGraph ZMQ.
    """

    class MyZMQGraphConfig(Config):
        addr: str
        zmq_topic: str
        output_filename: str

    class MyZMQGraph(Graph):
        ZMQ_SENDER: ZMQSender
        ZMQ_POLLER: ZMQPoller

        def setup(self) -> None:
            self.ZMQ_SENDER.configure(
                ZMQSenderConfig(
                    write_addr=self.config.addr,
                    zmq_topic=self.config.zmq_topic
                )
            )
            self.ZMQ_POLLER.configure(
                ZMQPollerConfig(
                    read_addr=self.config.addr,
                    zmq_topic=self.config.zmq_topic
                )
            )
            self.ZMQ_SENDER.setup()
            self.ZMQ_POLLER.setup()

        def process_nodes(self) -> None:
            asyncio.get_event_loop().run_until_complete(
                asyncio.wait(
                    [
                        self.ZMQ_POLLER.receive(),
                        self.ZMQ_SENDER.publish()
                    ]
                )
            )

        def _validate_topics(self) -> None:
            pass

    graph = MyZMQGraph()
    output_filename = get_test_filename()
    address = f"{ZMQ_ADDR}:{get_free_port()}"
    graph.configure(
        MyZMQGraphConfig(
            addr=address, zmq_topic=ZMQ_TOPIC, output_filename=output_filename
        )
    )
    graph.setup()
    graph.process_nodes()
    logger.info(f"Mean latency: {np.mean(graph.ZMQ_POLLER.latency)} ms")
    logger.info(f"Standard Deviation latency: {np.std(graph.ZMQ_POLLER.latency)} ms")


if __name__ == "__main__":
    test_labgraphz()