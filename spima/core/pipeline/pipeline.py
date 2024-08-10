from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import networkx as nx

from spima.core.datastore import BaseDataStore
from spima.core.datastore import InMemoryDataStore
from spima.core.node.base import BaseNode


@dataclass
class MergeInputsSetting:
    input_keys: List[str]
    merge_function: Callable


class Pipeline:
    def __init__(self, data_store: BaseDataStore = InMemoryDataStore()):
        self.data_store = data_store
        self.graph = nx.DiGraph()
        self.input_types: Dict[str, type] = dict()
        self.merge_input_settings: Dict[str, MergeInputsSetting] = dict()

    def add_node(self, node: BaseNode, input_key: Optional[str] = None, output_key: Optional[str] = None) -> None:
        if output_key is None:
            output_key = node.name
        self._register_input_type_from_node_output(output_key, node)
        self.graph.add_node(node.name, node=node, input_key=input_key, output_key=output_key)

    def _register_input_type_from_node_output(self, output_key: str, node: BaseNode) -> None:
        self.input_types[output_key] = node.run.__annotations__.get('return')

    def connect(self, from_node: BaseNode, to_node: BaseNode) -> None:
        self.graph.add_edge(from_node.name, to_node.name)

    def _validate_nodes_input(self) -> None:
        subgraphs: List[nx.DiGraph] = [self.graph.subgraph(subgraph_name) for subgraph_name in nx.weakly_connected_components(self.graph)]
        for subgraph in subgraphs:
            nodes_to_run = nx.topological_sort(subgraph)
            for node_name in nodes_to_run:
                node: BaseNode = subgraph.nodes[node_name]['node']
                input_key = subgraph.nodes[node_name]['input_key']
                if input_key is not None:
                    node_input_type = node.run.__annotations__.get('payload')
                    registered_input_type = self.input_types.get(input_key)
                    if registered_input_type is None:
                        raise ValueError(f"Input key {input_key} is not registered")
                    if node_input_type != registered_input_type:
                        raise ValueError(f"Node {node.name} expects input of type {node_input_type} but got {registered_input_type}")

    def get_node(self, node_name: str) -> BaseNode:
        return self.graph.nodes[node_name]['node']

    def _setup_inputs_data(self, inputs_data: Dict[str, Any]) -> None:
        for key, value in inputs_data.items():
            self.input_types[key] = type(value)
            self.data_store.set(key, value)

    def merge_inputs(self, input_keys: List[str], new_key: str, merge_function: Callable) -> None:
        self.input_types[new_key] = merge_function.__annotations__.get('return')
        self.merge_input_settings[new_key] = MergeInputsSetting(input_keys, merge_function)

    def run(self, inputs_data: Optional[Dict[str, Any]]=None) -> None:
        if inputs_data is not None:
            self._setup_inputs_data(inputs_data)
        self._validate_nodes_input()
        subgraphs: List[nx.DiGraph] = [self.graph.subgraph(subgraph_name) for subgraph_name in nx.weakly_connected_components(self.graph)]
        for subgraph in subgraphs:
            nodes_to_run = nx.topological_sort(subgraph)
            for node_name in nodes_to_run:
                node: BaseNode = subgraph.nodes[node_name]['node']
                input_key = subgraph.nodes[node_name]['input_key']
                output_key = subgraph.nodes[node_name]['output_key']
                payload = None
                if input_key is not None:
                    if input_key in self.merge_input_settings:
                        combined_data_setting = self.merge_input_settings[input_key]
                        payload = combined_data_setting.merge_function(*(self.data_store.get(key) for key in combined_data_setting.input_keys))
                    else:
                        payload = self.data_store.get(input_key)
                output = node.run(payload)
                self.data_store.set(output_key, output)

    def get_data(self, key: str) -> Any:
        return self.data_store.get(key)

    def clear_data(self) -> None:
        self.data_store.clear()

# # Example usage

# def merge_to_dict(a: int, source: dict) -> dict:
#     return {"a": a, "b": source["b"]}

# if __name__ == "__main__":
#     pipeline = Pipeline()

#     node1 = PrintNode("node1")
#     node2 = AddNode("node2", "a", "b")
#     node3 = PrintNode("node3")
#     node4 = AddNode("node4", "a", "b")

#     pipeline.add_node(node1)
#     pipeline.add_node(node2, input_key="source")
#     pipeline.add_node(node3)
#     pipeline.add_node(node4, input_key="combined")

#     pipeline.connect(node2, node4)

#     pipeline.merge_inputs(["node2", "source"], "combined", merge_to_dict)
#     pipeline.run({"source": {"a": 1, "b": 2}})

#     result = pipeline.get_data("node4")
#     print(result)
#     pipeline.clear_data()
