import ast
import pytest
from altryx_accelerator.models import Tool, Connection, Workflow
from altryx_accelerator.tools.base import GeneratorContext
from altryx_accelerator.tools.builtin import DynamicRenameConverter

MODES = [
    ('Add', '<AddPrefixSuffix><Type>Prefix</Type><Text>pre_</Text></AddPrefixSuffix>', 'pre_Name'),
    ('Add Suffix', '<Text>_post</Text>', 'Name_post'),
    ('Remove Prefix', '<Text>Na</Text>', 'me'),
    ('Remove Suffix', '<Text>me</Text>', 'Na'),
    ('UpperCase', '', 'NAME'),
    ('LowerCase', '', 'name'),
    ('TitleCase', '', 'Name'),
    ('Formula', '<Expression>UpperCase([_CurrentField_])</Expression>', 'NAME'),
    ('Formula', '<Expression>[_CurrentField_] + &quot;_suffix&quot;</Expression>', 'Name_suffix'),
    ('FirstRow', '', 'Heading'),
    ('RightInputRows', '<NamesFromRows><InputMode>Mapped</InputMode><OldName>old</OldName><NewName>new</NewName></NamesFromRows>', 'Renamed'),
]

def make_code(mode, config):
    tool = Tool(3,'','DynamicRename',{},f'<Configuration><RenameMode>{mode}</RenameMode><Fields><Field name="Name"/><Field name="keep" selected="False"/></Fields>{config}</Configuration>','')
    inputs = [Tool(i,'','TextInput',{},'<Configuration/>','') for i in (1,2)]
    tools = inputs + [tool]
    connections = [Connection(1,'Output',3,'Targets'), Connection(2,'Output',3,'Source')]
    wf = Workflow([],{}, {t.tool_id:t for t in tools}, connections, {})
    ctx = GeneratorContext(wf,tools,connections)
    ctx.set_output_var(1,'df_left');ctx.set_output_var(2,'df_right')
    return '\n'.join(DynamicRenameConverter().convert(tool,ctx))

@pytest.mark.parametrize('mode,config,expected', MODES)
def test_modes_emit_python(mode,config,expected):
    code = make_code(mode,config)
    ast.parse(code)
    assert 'TODO' not in code
    assert '.select(' in code

@pytest.mark.parametrize('mode,config,expected', MODES)
def test_modes_values(mode,config,expected,spark):
    from pyspark.sql import functions as F
    ns = {'spark':spark, 'F':F,
          'df_left':spark.createDataFrame([('Heading','x'),('data','y')],['Name','keep']),
          'df_right':spark.createDataFrame([('Name','Renamed')],['old','new'])}
    exec(make_code(mode,config),ns)
    assert ns['df_3'].columns == [expected,'keep']
    assert ns['df_left'].count() == 2
    assert ns['df_3'].count() == (1 if mode == 'FirstRow' else 2)
    if mode == 'FirstRow':
        assert tuple(ns['df_3'].first()) == ('data','y')
