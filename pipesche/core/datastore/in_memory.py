from typing import Any, Dict

from pipesche.core.datastore.base import BaseDataStore


class InMemoryDataStore(BaseDataStore):
    def __init__(self) -> None:
        self.data: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def exist(self, key: str) -> bool:
        return key in self.data

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def delete(self, key: str) -> None:
        del self.data[key]

    def clear(self) -> None:
        self.data.clear()
