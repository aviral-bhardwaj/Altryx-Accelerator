"""Embed the built wheel (<300 KB) as a deterministic offline zip fallback."""
import base64
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
wheel = ROOT / 'dist/altryx_accelerator-1.1.0-py3-none-any.whl'
payload = base64.b64encode(wheel.read_bytes()).decode() if wheel.stat().st_size < 300_000 else ''
for path in sorted((ROOT / 'notebooks').glob('*.ipynb')):
    nb = json.loads(path.read_text())
    for cell in nb['cells']:
        source = ''.join(cell['source'])
        if 'OFFLINE_WHEEL = ' in source:
            source = re.sub(r'OFFLINE_WHEEL = "[^"\n]*"', lambda _: f'OFFLINE_WHEEL = "{payload}"', source)
            cell['source'] = source.splitlines(keepends=True)
    path.write_text(json.dumps(nb, indent=1)+'\n')
print(f'Embedded offline wheel: {wheel.stat().st_size} bytes' if payload else 'Wheel >=300 KB: offline fallback omitted; upload wheel to Volume.')
