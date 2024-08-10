from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class BaseNode(ABC, Generic[T, U]):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def run(self, payload: T) -> U:
        raise NotImplementedError("Subclasses should implement this method.")
