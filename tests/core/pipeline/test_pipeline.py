import logging
from dataclasses import dataclass
from typing import List

import pytest

from spima.core.node import BaseNode
from spima.core.pipeline import Pipeline


logger = logging.getLogger(__name__)


@dataclass
class AddPayload:
    x: int
    y: int


@dataclass
class MessagePayload:
    messages: List[str]


class PrintNode(BaseNode[None, None]):
    def __init__(self, name):
        super().__init__(name)

    def run(self, payload: None=None) -> None:
        message = "No message found"
        logger.debug(f"Node {self.name}: {message}")

class AddNode(BaseNode[AddPayload, int]):
    def run(self, payload: AddPayload) -> int:
        result = payload.x + payload.y
        logger.debug(f"Node {self.name}: {payload.x} + {payload.y} = {result}")
        return result

class MessageNode(BaseNode[MessagePayload, str]):
    def run(self, payload: MessagePayload) -> str:
        message = " ".join(payload.messages)
        logger.debug(f"Node {self.name}: {message}")
        return message


def test_pipeline_running_with_correct_sequence():
    class Node(BaseNode[int, None]):
        results = []

        def __init__(self, name):
            super().__init__(name)

        def run(self, payload: int) -> None:
            self.results.append(payload)

    node1 = Node("node1")
    node2 = Node("node2")
    node3 = Node("node3")
    node4 = Node("node4")

    pipeline = Pipeline()
    pipeline.add_node(node1, input_key="input_1")
    pipeline.add_node(node2, input_key="input_2")
    pipeline.add_node(node3, input_key="input_3")
    pipeline.add_node(node4, input_key="input_4")

    pipeline.connect(node1, node4)
    pipeline.connect(node2, node4)
    pipeline.connect(node4, node3)

    pipeline.run({"input_1": 1, "input_2": 2, "input_3": 3, "input_4": 4})

    possible_results_1 = [1, 4, 2, 3]
    possible_results_2 = [1, 2, 4, 3]
    if Node.results != possible_results_1 and Node.results != possible_results_2:
        assert False, f"Expected results to be {possible_results_1} or {possible_results_2}, but got {Node.results}"


def test_pipeline_running_with_merge_inputs():
    node1 = PrintNode("node1")
    node2 = AddNode("node2")
    node3 = PrintNode("node3")
    node4 = AddNode("node4")
    node5 = MessageNode("node5")

    pipeline = Pipeline()
    pipeline.add_node(node1)
    pipeline.add_node(node2, input_key="source")
    pipeline.add_node(node3)
    pipeline.add_node(node4, input_key="combine.source.node2")
    pipeline.add_node(node5, input_key="combine.node2.node4")
    pipeline.connect(node2, node4)
    pipeline.connect(node4, node5)

    def merge_add_payload_and_number(payload: AddPayload, number: int) -> AddPayload:
        return AddPayload(payload.x, payload.y + number)

    def merge_numbers(x: int, y: int) -> MessagePayload:
        return MessagePayload([str(x), str(y)])

    pipeline.merge_inputs(["source", "node2"], "combine.source.node2", merge_add_payload_and_number)
    pipeline.merge_inputs(["node2", "node4"], "combine.node2.node4", merge_numbers)

    pipeline.run({"source": AddPayload(1, 2)})

    assert pipeline.get_data("node2") == 3
    assert pipeline.get_data("node4") == 6
    assert pipeline.get_data("node5") == "3 6"


def test_node_name_register_should_be_unique():
    node1 = AddNode("node")
    node2 = AddNode("node")

    pipeline = Pipeline()
    pipeline.add_node(node1)

    with pytest.raises(ValueError):
        pipeline.add_node(node2)


def test_node_output_key_register_should_be_unique():
    node1 = AddNode("node1")
    node2 = AddNode("node2")

    pipeline = Pipeline()
    pipeline.add_node(node1, output_key="output")

    with pytest.raises(ValueError):
        pipeline.add_node(node2, output_key="output")


def test_new_key_generate_by_merge_inputs_should_be_unique():
    node1 = AddNode("node1")
    node2 = AddNode("node2")

    pipeline = Pipeline()
    pipeline.add_node(node1, input_key="input_1")
    pipeline.add_node(node2, input_key="input_2")

    def merge_function(x: AddPayload, y: AddPayload) -> AddPayload:
        return x

    with pytest.raises(ValueError):
        pipeline.merge_inputs(["input_1", "input_2"], "node1", merge_function)


def test_pipeline_input_keys_should_not_conflict_with_existed_keys():
    node1 = AddNode("node1")
    node2 = AddNode("node2")

    pipeline = Pipeline()
    pipeline.add_node(node1, input_key="input_1")
    pipeline.add_node(node2, input_key="input_2")

    with pytest.raises(ValueError):
        pipeline.run({"input_1": AddPayload(x=1, y=2), "input_2": AddPayload(x=3, y=4), "node1": AddPayload(x=5, y=6)})


def test_pipeline_running_with_clear_data():
    node1 = AddNode("node1")
    node2 = AddNode("node2")

    pipeline = Pipeline()
    pipeline.add_node(node1, input_key="input_1")
    pipeline.add_node(node2, input_key="input_2")

    pipeline.connect(node1, node2)
    pipeline.run({"input_1": AddPayload(x=1, y=2), "input_2": AddPayload(x=3, y=4)})

    assert pipeline.get_data("node2") == 7

    pipeline.clear_data()

    with pytest.raises(ValueError):
        pipeline.get_data("node2")
