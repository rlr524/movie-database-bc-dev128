from typing import Protocol

class MovieRepository(Protocol):
    def connect(self):
        pass
