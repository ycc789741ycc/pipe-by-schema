from abc import ABC, abstractmethod
from typing import Any


class BaseDataStore(ABC):
    @abstractmethod
    def get(self, key: str) -> Any:
        raise NotImplementedError("Subclasses should implement this method.")

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        raise NotImplementedError("Subclasses should implement this method.")

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError("Subclasses should implement this method.")
    
    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError("Subclasses should implement this method.")
