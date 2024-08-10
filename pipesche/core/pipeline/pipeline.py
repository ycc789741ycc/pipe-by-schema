import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set

import networkx as nx

from pipesche.core.datastore import BaseDataStore, InMemoryDataStore
from pipesche.core.node.base import BaseNode

logger = logging.getLogger(__name__)


@dataclass
class MergeInputsSetting:
    input_keys: List[str]
    merge_function: Callable


class Pipeline:
    def __init__(self, data_store: BaseDataStore = InMemoryDataStore()):
        self.data_store = data_store
        self._graph = nx.DiGraph()
        self._data_types: Dict[str, Optional[type]] = dict()
        self._merge_inputs_settings: Dict[str, MergeInputsSetting] = dict()
        self._registered_node_names: Set[str] = set()
        self._registered_keys: Set[str] = set()

    def add_node(
        self,
        node: BaseNode,
        input_key: Optional[str] = None,
        output_key: Optional[str] = None,
    ) -> None:
        if output_key is None:
            output_key = node.name
        self._validate_node_name_and_output_key_is_unique(node.name, output_key)
        self._register_node_name_and_output_key(node.name, output_key)
        self._register_output_type(output_key, node)
        self._graph.add_node(node.name, node=node, input_key=input_key, output_key=output_key)

    def _validate_node_name_and_output_key_is_unique(self, node_name: str, output_key: str) -> None:
        if node_name in self._registered_node_names:
            raise ValueError(f"Node name {node_name} is already registered")
        if output_key in self._registered_keys:
            raise ValueError(f"Output key {output_key} is already registered")

    def _register_node_name_and_output_key(self, node_name: str, output_key: str) -> None:
        self._registered_node_names.add(node_name)
        self._registered_keys.add(output_key)

    def _register_output_type(self, output_key: str, node: BaseNode) -> None:
        self._data_types[output_key] = node.run.__annotations__.get("return")

    def connect(self, from_node: str, to_node: str) -> None:
        """Connect two nodes in the pipeline by node name.

        Args:
            from_node (str): Node name of the source node.
            to_node (str): Node name of the destination node.
        """
        self._graph.add_edge(from_node, to_node)

    def _validate_nodes_input(self) -> None:
        subgraphs: List[nx.DiGraph] = [
            self._graph.subgraph(subgraph_name) for subgraph_name in nx.weakly_connected_components(self._graph)
        ]
        for subgraph in subgraphs:
            nodes_to_run = nx.topological_sort(subgraph)
            for node_name in nodes_to_run:
                node: BaseNode = subgraph.nodes[node_name]["node"]
                input_key = subgraph.nodes[node_name]["input_key"]
                if input_key is not None:
                    node_input_type = node.run.__annotations__.get("payload")
                    registered_input_type = self._data_types.get(input_key)
                    if registered_input_type is None:
                        raise ValueError(f"Input key {input_key} is not registered")
                    if node_input_type != registered_input_type:
                        raise ValueError(
                            f"Node {node.name} expects input of type {node_input_type} but got {registered_input_type}"
                        )

    def get_node(self, node_name: str) -> BaseNode:
        return self._graph.nodes[node_name]["node"]

    def merge_inputs(self, input_keys: List[str], new_key: str, merge_function: Callable) -> None:
        if new_key in self._registered_keys:
            raise ValueError(f"Key {new_key} is already registered")

        self._data_types[new_key] = merge_function.__annotations__.get("return")
        self._registered_keys.add(new_key)
        self._merge_inputs_settings[new_key] = MergeInputsSetting(input_keys, merge_function)

    def run(self, inputs_data: Optional[Dict[str, Any]] = None) -> None:
        if inputs_data is not None:
            self._setup_inputs_data(inputs_data)

        self._validate_nodes_input()
        subgraphs: List[nx.DiGraph] = [
            self._graph.subgraph(subgraph_name) for subgraph_name in nx.weakly_connected_components(self._graph)
        ]
        for subgraph in subgraphs:
            nodes_to_run = list(nx.topological_sort(subgraph))
            logger.info(
                "Running pipeline in topological order: " + " -> ".join([node_name for node_name in nodes_to_run])
            )
            for node_name in nodes_to_run:
                node: BaseNode = subgraph.nodes[node_name]["node"]
                input_key: str = subgraph.nodes[node_name]["input_key"]
                output_key: str = subgraph.nodes[node_name]["output_key"]
                payload = self._get_payload_by_input_key(input_key) if input_key is not None else None
                output = node.run(payload)
                self.data_store.set(output_key, output)

        if inputs_data is not None:
            self._remove_inputs_data(inputs_data)

    def _setup_inputs_data(self, inputs_data: Dict[str, Any]) -> None:
        for key, value in inputs_data.items():
            if key in self._registered_keys:
                raise ValueError(f"Key {key} is already registered")
            self._data_types[key] = type(value)
            self.data_store.set(key, value)

    def _remove_inputs_data(self, inputs_data: Dict[str, Any]) -> None:
        for key in inputs_data.keys():
            del self._data_types[key]
            self.data_store.delete(key)

    def _get_payload_by_input_key(self, input_key: str) -> Any:
        if input_key in self._merge_inputs_settings:
            combined_data_setting = self._merge_inputs_settings[input_key]
            payload = combined_data_setting.merge_function(
                *(self.data_store.get(key) for key in combined_data_setting.input_keys)
            )
        else:
            payload = self.data_store.get(input_key)
        return payload

    def get_data(self, key: str) -> Any:
        if not self.data_store.exist(key):
            raise ValueError(f"Key {key} is not existed in data store")
        return self.data_store.get(key)

    def clear_data(self) -> None:
        self.data_store.clear()

    def clear_nodes(self) -> None:
        self._graph.clear()
        self._data_types.clear()
        self._merge_inputs_settings.clear()
        self._registered_node_names.clear()
        self._registered_keys.clear()
