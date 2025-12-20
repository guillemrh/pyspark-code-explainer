# backend/app/parsers/ast_parser.py

import ast
from typing import List, Dict
from .dag_nodes import SparkOperationNode


class PySparkASTParser(ast.NodeVisitor):
    """
    AST parser that extracts Spark DataFrame operations
    from single-file PySpark scripts.
    """

    def __init__(self):
        self.operations: List[SparkOperationNode] = []
        self.variable_lineage: Dict[str, List[str]] = {}

    def visit_Assign(self, node: ast.Assign):
        """
        Handles patterns like:
        df2 = df1.select(...).filter(...)
        """
        if not isinstance(node.value, ast.Call):
            return

        # Only support simple assignments for v1
        if not isinstance(node.targets[0], ast.Name):
            return

        target_df = node.targets[0].id

        call_chain = self._extract_call_chain(node.value)
        if not call_chain:
            return

        parent_df = call_chain[0]["base"]

        self.variable_lineage[target_df] = [parent_df]

        node_id = f"{target_df}_{call_chain[-1]['op']}_{node.lineno}"

        for call in call_chain:
            op_node = SparkOperationNode(
                id=node_id,
                df_name=target_df,
                operation=call["op"],
                parents=[parent_df],
                lineno=node.lineno,
            )
            self.operations.append(op_node)

        self.generic_visit(node)

    def _extract_call_chain(self, call: ast.Call) -> List[dict]:
        """
        Extract chained calls like:
            df.select().filter().groupBy()

        Returns:
        [
          {"base": "df", "op": "select"},
          {"base": "df", "op": "filter"},
          {"base": "df", "op": "groupBy"}
        ]
        """
        chain = []
        current = call

        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                op_name = current.func.attr
                current = current.func.value
                chain.append({"op": op_name, "base": self._get_base_name(current)})
            else:
                break

        return list(reversed(chain))

    def _get_base_name(self, node) -> str:
        """
        Extract base DataFrame variable name.
        """
        if isinstance(node, ast.Name):
            return node.id
        return "UNKNOWN"
