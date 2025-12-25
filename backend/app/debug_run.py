import ast

from app.parsers.ast_parser import PySparkASTParser
from app.services.operation_dag_builder import build_operation_dag
from app.visualizers.dag_visualizer import render_operation_dag_to_dot


code = """
df2 = df.select("a").filter("a > 5")
df3 = df2.groupBy("a").count()
df4 = df3.join(df2, on="a", how="inner")
"""

# --------------------
# AST PARSING
# --------------------
tree = ast.parse(code)
parser = PySparkASTParser()
parser.visit(tree)

print("\n=== AST OUTPUT ===")
for op in parser.operations:
    print(op)

# --------------------
# DAG BUILDING
# --------------------
dag = build_operation_dag(parser.operations)

print("\n=== DAG NODES ===")
for node_id, node in dag.nodes.items():
    print(
        f"Node {node_id}: "
        f"label={node.label}, "
        f"parents={node.parents}, "
        f"children={node.children}"
    )

# --------------------
# GRAPHVIZ DOT
# --------------------
dot = render_operation_dag_to_dot(dag)

print("\n=== GRAPHVIZ DOT ===")
print(dot)
