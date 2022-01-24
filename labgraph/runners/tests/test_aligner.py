#!/usr/bin/env python3
# Copyright 2004-present Facebook. All Rights Reserved.

import asyncio
import time
import json
import os
from typing import List, Dict, Sequence

import pytest

from ...graphs.graph import Graph
from ...graphs.group import Connections
from ...graphs.method import AsyncPublisher, publisher, subscriber
from ...graphs.module import Module
from ...graphs.node import Node
from ...graphs.topic import Topic
from ...graphs.config import Config
from ...messages.message import TimestampedMessage
from ...util.testing import get_test_filename, local_test
from ..aligner import TimestampAligner
from ..exceptions import NormalTermination
from ..parallel_runner import ParallelRunner
from ..local_runner import LocalRunner
from ..runner import RunnerOptions


NUM_MESSAGES = 5
FAST_PUBLISH_RATE = 10
SLOW_PUBLISH_RATE = 1

SMALL_ALIGN_LAG = 0.01
LARGE_ALIGN_LAG = 5

OUTPUT_FILENAME = get_test_filename("json")


class MyMessage1(TimestampedMessage):
    int_field: int


class MyMessage2(TimestampedMessage):
    int_field: int


class MySource1(Node):
    A = Topic(MyMessage1)

    @publisher(A)
    async def source(self) -> AsyncPublisher:
        latest = time.time()
        for i in reversed(range(NUM_MESSAGES)):
            yield self.A, MyMessage1(timestamp=latest - i, int_field=i)
            await asyncio.sleep(1 / SLOW_PUBLISH_RATE)


class MySource2(Node):
    A = Topic(MyMessage2)

    @publisher(A)
    async def source(self) -> AsyncPublisher:
        latest = time.time()
        for i in reversed(range(NUM_MESSAGES)):
            yield self.A, MyMessage2(timestamp=latest - i, int_field=i)
            await asyncio.sleep(1 / FAST_PUBLISH_RATE)


class MySinkConfig(Config):
    output_filename: str


class MySink(Node):
    D = Topic(MyMessage1)
    E = Topic(MyMessage2)
    config: MySinkConfig

    def setup(self) -> None:
        self.messages_seen: int = 0

    @subscriber(D)
    def sink1(self, message: MyMessage1) -> None:
        with open(self.config.output_filename, "a") as output_file:
            output_file.write(json.dumps(message.asdict()) + "\n")
            self.messages_seen += 1
            if self.messages_seen == 2 * NUM_MESSAGES:
                raise NormalTermination()

    @subscriber(E)
    def sink2(self, message: MyMessage2) -> None:
        with open(self.config.output_filename, "a") as output_file:
            output_file.write(json.dumps(message.asdict()) + "\n")
            self.messages_seen += 1
            if self.messages_seen == 2* NUM_MESSAGES:
                raise NormalTermination()


class MyGraph(Graph):
    SOURCE1: MySource1
    SOURCE2: MySource2
    SINK: MySink

    config: MySinkConfig

    def setup(self) -> None:
        self.SINK.configure(self.config)

    def connections(self) -> Connections:
        return ((self.SOURCE1.A, self.SINK.D), (self.SOURCE2.A, self.SINK.E))

    def process_modules(self) -> Sequence[Module]:
        return (self.SOURCE1, self.SOURCE2, self.SINK)


@local_test
def test_slow_align_interval() -> None:
    """
    Tests that when the default timestamp aligner is specified for a runner
    with insufficient time lag, the results from its streams should arrive in
    expected (not chronological) order.
    """

    graph = MyGraph(MySinkConfig(output_filename=OUTPUT_FILENAME))
    aligner = TimestampAligner(SMALL_ALIGN_LAG)
    runner = ParallelRunner(graph=graph, options=RunnerOptions(aligner=aligner))
    runner.run()

    results = []
    with open(OUTPUT_FILENAME, "r") as output_file:
        lines = output_file.readlines()
        for line in lines:
            results.append(json.loads(line)["int_field"])
    os.remove(OUTPUT_FILENAME)
    assert len(results) == NUM_MESSAGES * 2
    assert results != sorted(results, reverse=True)


@local_test
def test_align_two_streams() -> None:
    """
    Tests that when the default timestamp aligner is specified for a runner
    with sufficient time lag, the results from all its streams should arrive
    in chronological order.
    """

    graph = MyGraph(MySinkConfig(output_filename=OUTPUT_FILENAME))
    aligner = TimestampAligner(LARGE_ALIGN_LAG)
    runner = ParallelRunner(graph=graph, options=RunnerOptions(aligner=aligner))
    runner.run()

    results = []
    with open(OUTPUT_FILENAME, "r") as output_file:
        lines = output_file.readlines()
        for line in lines:
            results.append(json.loads(line)["int_field"])
    os.remove(OUTPUT_FILENAME)
    assert len(results) == NUM_MESSAGES * 2
    assert results == sorted(results, reverse=True)
