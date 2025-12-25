import ast
from app.parsers.ast_parser import PySparkASTParser
from app.services.operation_dag_builder import build_operation_dag
from app.visualizers.dag_visualizer import render_operation_dag_to_dot


def run_dag_pipeline(code: str) -> dict:
    """
    Runs the OPERATION-LEVEL pipeline only:

    PySpark code
        -> AST parsing
        -> Operation DAG (execution / performance view)
        -> Graphviz DOT
    """

    # Parse code into AST
    tree = ast.parse(code)

    # Extract Spark operations
    parser = PySparkASTParser()
    parser.visit(tree)

    # Build execution / operation DAG
    operation_dag = build_operation_dag(parser.operations)

    # Render DAG to Graphviz DOT
    dag_dot = render_operation_dag_to_dot(operation_dag)

    return {
        "dag_dot": dag_dot,
    }