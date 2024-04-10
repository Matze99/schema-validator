from abc import ABC, abstractmethod


class SchemaValidator(ABC):
    @abstractmethod
    def validate(self, dataframe) -> bool:
        raise NotImplementedError
