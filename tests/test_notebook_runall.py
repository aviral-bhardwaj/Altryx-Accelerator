"""
"Run All" test for the Skill Mode notebook: executes every code cell in order
(as a Databricks/Jupyter user would) with default settings and asserts the
converted notebook + summary land in the output directory.

Needs no Spark — the default configuration converts the bundled sample and
skips sample-data execution.
"""

import json
from pathlib import Path

import nbformat
import pytest

NOTEBOOK = Path(__file__).parent.parent / "notebooks" / "01_Skill_Mode_Alteryx_to_Databricks.ipynb"


def test_notebook_is_valid_nbformat():
    nb = nbformat.read(str(NOTEBOOK), as_version=4)
    nbformat.validate(nb)
    code_cells = [c for c in nb.cells if c.cell_type == "code"]
    md_cells = [c for c in nb.cells if c.cell_type == "markdown"]
    assert len(code_cells) >= 9, "expected one code cell per UX section"
    assert md_cells and "Skill Mode" in md_cells[0].source


def test_notebook_run_all(tmp_path, monkeypatch):
    monkeypatch.setenv("SKILL_OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    nb = json.loads(NOTEBOOK.read_text())
    namespace = {"__name__": "__main__"}
    executed = 0
    for index, cell in enumerate(nb["cells"]):
        if cell["cell_type"] != "code":
            continue
        source = cell["source"] if isinstance(cell["source"], str) else "".join(cell["source"])
        # %pip / %md magics are Databricks/IPython concerns, not plain Python.
        source = "\n".join(
            line for line in source.splitlines() if not line.strip().startswith("%"))
        if not source.strip():
            continue
        exec(compile(source, f"<cell {index}>", "exec"), namespace)
        executed += 1

    assert executed >= 9

    # Default widgets convert tests/sample_workflows/02_join_summarize.yxmd.
    exported_nb = tmp_path / "02_join_summarize.ipynb"
    exported_py = tmp_path / "02_join_summarize.py"
    summary_file = tmp_path / "conversion_summary.json"
    assert exported_nb.exists() and exported_py.exists() and summary_file.exists()

    summary = json.loads(summary_file.read_text())
    assert summary["02_join_summarize"]["status"] in ("PASS", "WARNING")
    assert summary["02_join_summarize"]["unsupported"] == []

    # The exported notebook is valid and leads with the validation report.
    exported = nbformat.read(str(exported_nb), as_version=4)
    nbformat.validate(exported)
    assert exported.cells[0].cell_type == "markdown"
    assert "Conversion Validation Report" in exported.cells[0].source
    assert exported.cells[0].source.startswith("## ✅") or "PASS" in exported.cells[0].source


def test_quickstart_run_all(tmp_path, monkeypatch):
    """All quickstart cells run locally; bootstrap uses the installed distribution."""
    monkeypatch.setenv('SKILL_OUTPUT_DIR', str(tmp_path))
    notebook = NOTEBOOK.with_name('02_Quickstart_Convert_Sample_Workflows.ipynb')
    nb = nbformat.read(notebook, as_version=4)
    namespace = {'__name__': '__main__'}
    for cell in nb.cells:
        if cell.cell_type == 'code' and 'bootstrap' not in cell.metadata.get('tags', []):
            exec(compile(cell.source, str(notebook), 'exec'), namespace)
    summary = json.loads((tmp_path/'conversion_summary.json').read_text())
    assert len(summary) == 4
    assert all(v['status'] in ('PASS','WARNING') and not v['unsupported'] for v in summary.values())
    assert len(list(tmp_path.glob('*.ipynb'))) == 4
    assert len(list(tmp_path.glob('*.py'))) == 4


def test_quickstart_from_installed_wheel(tmp_path, monkeypatch):
    """A stranger's wheel install has no checkout on its import path."""
    import os
    import subprocess
    import sys
    root = NOTEBOOK.parents[1]
    wheel = root / 'dist/altryx_accelerator-1.1.0-py3-none-any.whl'
    if not wheel.exists():
        subprocess.run([sys.executable, '-m', 'build', '--wheel', '--no-isolation', str(root)], check=True, capture_output=True)
    install = tmp_path/'installed'
    subprocess.run([sys.executable, '-m', 'pip', 'install', '--no-deps', '--target', str(install), str(wheel)], check=True, capture_output=True)
    script = tmp_path/'run_quickstart.py'
    script.write_text('''import json,sys
sys.path.insert(0, sys.argv[1])
import altryx_accelerator
assert altryx_accelerator.__version__ == "1.1.0"
assert altryx_accelerator.__file__.startswith(sys.argv[1])
nb = json.load(open(sys.argv[2]))
namespace = {"__name__": "__main__"}
for cell in nb["cells"]:
    if cell["cell_type"] == "code" and "bootstrap" not in cell["metadata"].get("tags", []):
        exec("".join(cell["source"]), namespace)
''')
    monkeypatch.setenv('SKILL_OUTPUT_DIR', str(tmp_path/'outputs'))
    subprocess.run([sys.executable, '-I', str(script), str(install), str(NOTEBOOK.with_name('02_Quickstart_Convert_Sample_Workflows.ipynb'))], cwd=tmp_path, env=os.environ.copy(), check=True, capture_output=True)
    assert len(list((tmp_path/'outputs').glob('*.ipynb'))) == 4
