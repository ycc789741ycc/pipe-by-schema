# Pipe-by-Schema

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Using defined data schema to passing between nodes of pipeline, much more easier to maintain your complex workflow.

## Installation
* Python 3.9+
* poetry 1.7.1
```bash
poetry install
```

## Quick Start
1. Implement the `run` method of node class and define input and output schema.
```python
from dataclasses import dataclass

from pipesche import BaseNode


@dataclass
class AddPayload:
    x: int
    y: int


class Add(BaseNode[AddPayload, int]):
    def run(self, payload: AddPayload) -> int:
        result = payload.x + payload.y
        print(f"{payload.x} + {payload.y} = {result}")
        return result


class PrintSomeThing(BaseNode[None, None]):
    def run(self, payload: None) -> None:
        print("Some thing...")
```

2. Add the nodes to pipeline and link the nodes.
```python
from pipesche import Pipeline


node_1 = Add("node_1")
node_2 = PrintSomeThing("node_2")
node_3 = Add("node_3")

pipeline = Pipeline()
pipeline.add_node(node_1, input_key="input_1")
pipeline.add_node(node_2)
pipeline.add_node(node_3, input_key="input_2")
pipeline.connect(node_1.name, node_3.name)
pipeline.connect(node_3.name, node_2.name)

pipeline.run({"input_1": AddPayload(1, 2), "input_2": AddPayload(3, 4)})

# Output:
# 1 + 2 = 3
# 3 + 4 = 7
# Some thing...
```

3. Get the result of the node.
```python
pipeline.get_data(key=node_3.name)

# Output:
# 7
```
