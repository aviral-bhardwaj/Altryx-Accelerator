"""Deterministic shipped artifacts plus execution of every sample's generated code."""
import ast
import json
from pathlib import Path
import pytest
from altryx_accelerator.parser import AlteryxWorkflowParser
from altryx_accelerator.self_correction import SelfCorrectingConverter
from altryx_accelerator.converter_engine import ConverterEngine
from altryx_accelerator.databricks_exporter import DatabricksExporter, to_ipynb
from altryx_accelerator.validators import strip_databricks_magics

SAMPLES = sorted((Path(__file__).parent / 'sample_workflows').glob('*.yxmd'))

@pytest.mark.parametrize('file', SAMPLES, ids=lambda p: p.stem)
def test_sample_outputs(file):
    workflow = AlteryxWorkflowParser(str(file)).parse()
    result = SelfCorrectingConverter().convert(workflow, file.stem)
    assert result.status in ('PASS', 'WARNING'), result.report_markdown()
    assert result.conversion.unsupported_tools == []
    ast.parse(strip_databricks_magics(result.code))
    assert 'TODO placeholder' not in result.code
    assert 'spark.table("TODO.' not in result.code
    target = Path(__file__).parent / 'output' / file.stem
    DatabricksExporter(str(target)).export(result)
    report = {'workflow': file.stem, 'status': result.status, 'unsupported': [],
              'tools': result.conversion.num_tools,
              'issues': [i.to_dict() for i in result.structural_report.issues]}
    (target / f'{file.stem}_report.json').write_text(json.dumps(report, indent=2) + '\n')
    # Generated notebook cell IDs and source must be byte-for-byte stable.
    assert to_ipynb(result.code) == to_ipynb(result.code)

@pytest.mark.parametrize('file', SAMPLES, ids=lambda p: p.stem)
def test_sample_spark_execution(file, spark):
    from altryx_accelerator.sample_data import register_sample_sources
    workflow = AlteryxWorkflowParser(str(file)).parse()
    mappings = register_sample_sources(workflow, spark)
    result = SelfCorrectingConverter(engine=ConverterEngine(source_tables_config=mappings)).convert(workflow, file.stem)
    ns = {'spark': spark}
    for cell in to_ipynb(result.code)['cells']:
        if cell['cell_type'] == 'code':
            exec(''.join(cell['source']), ns)
    final_id = {'01_simple_filter_select':4, '02_join_summarize':5, '03_formula_union_sort':7, 'usi_misc_long_format':93}[file.stem]
    if file.stem == 'usi_misc_long_format':
        # Browse is the terminal inspection tool in this workflow.
        final_id = next(t.tool_id for t in workflow.all_tools.values() if t.tool_type in ('Browse','BrowseV2'))
    final = ns[f'df_{final_id}']
    expected = {'01_simple_filter_select':2, '02_join_summarize':2, '03_formula_union_sort':4, 'usi_misc_long_format':7}[file.stem]
    assert final.count() == expected
    assert final.columns and len(final.columns) == len(set(final.columns))
    if file.stem == 'usi_misc_long_format':
        assert {'misc_gid_respondent', 'misc_question_grp', 'misc_product_ownership_old'} <= set(final.columns)
