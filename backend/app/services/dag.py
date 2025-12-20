# backend/app/services/dag_service.py

import ast
from typing import List
from app.parsers.ast_parser import PySparkASTParser
from app.parsers.dag_nodes import DAGNode


def build_dag_from_code(code: str) -> List[DAGNode]:
    """
    Entry point for DAG extraction.
    """
    tree = ast.parse(code)
    parser = PySparkASTParser()
    parser.visit(tree)
    return list(parser.nodes.values())
