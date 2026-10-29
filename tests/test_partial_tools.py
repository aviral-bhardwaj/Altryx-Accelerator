"""Every documented partial tool must at least yield an executable DataFrame."""
import ast
import pytest
from altryx_accelerator.converter_engine import load_tool_mapping
from altryx_accelerator.models import Tool, Connection, Workflow
from altryx_accelerator.tools import GeneratorContext, build_converter_map

PARTIAL = [name for name, spec in load_tool_mapping()['tools'].items() if spec['support'] == 'partial']

@pytest.mark.parametrize('tool_type', PARTIAL)
def test_partial_tools_have_executable_fallback(tool_type, spark):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    source = Tool(1,'','TextInput',{},'<Configuration/>','')
    target = Tool(2,'',tool_type,{},'<Configuration/>','')
    connection = Connection(1,'Output',2,'Input')
    workflow = Workflow([],{}, {1:source,2:target}, [connection], {})
    ctx = GeneratorContext(workflow,[source,target],[connection])
    ctx.set_output_var(1,'df_source')
    code = '\n'.join(build_converter_map()[tool_type].convert(target,ctx))
    ast.parse(code)
    frame = spark.createDataFrame([(1,)],['value'])
    frame.createOrReplaceTempView('partial_source')
    ns = {'spark':spark,'F':F,'Window':Window,'df_source':frame,'_resolve_source':lambda _: 'partial_source'}
    exec(code,ns)
    final = ns[ctx.df_vars[2]['Output']]
    assert final.count() == (0 if tool_type == 'GenerateRows' else 1)
    assert final.columns
