from app.parsers.ast_parser import PySparkASTParser
import ast

code = """
df2 = df.select("a").filter(col("a") > 5)
df3 = df2.groupBy("a").count()
"""

tree = ast.parse(code)
parser = PySparkASTParser()
parser.visit(tree)

for op in parser.operations:
    print(op)

def test_join_parsing():
    code = """
    df3 = df1.join(df2, on="id").select("a")
    """
    parser = PySparkASTParser()
    nodes = parser.parse(code)

    join_node = nodes[0]

    assert join_node.operation == "join"
    assert set(join_node.parents) == {"df1", "df2"}
