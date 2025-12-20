from dataclasses import dataclass
from typing import List


@dataclass
class SparkOperationNode:
    id: str
    df_name: str
    operation: str
    parents: List[str]
    lineno: int
