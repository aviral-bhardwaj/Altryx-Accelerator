#!/usr/bin/env python3
"""Validate or upload the release assets and reconcile a Delta Sharing share."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parents[1]
VERSION = '1.1.0'
WHEEL = f'altryx_accelerator-{VERSION}-py3-none-any.whl'


def validate_assets():
    import yaml
    import nbformat
    from PIL import Image
    listing = yaml.safe_load((ROOT / 'marketplace/listing.yaml').read_text())['listing']
    assert listing['version'] == VERSION
    assert listing['provider_name'] == 'Vrahad Analytics'
    notebooks = [ROOT / p for p in listing['assets']['notebooks']]
    assert len(notebooks) == 4 and len(notebooks) <= 10
    for path in notebooks:
        nb = nbformat.read(path, as_version=4)
        nbformat.validate(nb)
        sources = '\n'.join(c.source for c in nb.cells)
        assert 'releases/download/v1.1.0/' in sources
        assert '__OFFLINE_WHEEL__' not in sources, 'Run marketplace/build_notebooks.py after building the wheel'
        assert 'BUNDLE_ROOT' not in sources
    wheel = ROOT / 'dist' / WHEEL
    with zipfile.ZipFile(wheel) as archive:
        assert 'altryx_accelerator/config/tool_mapping.yaml' in archive.namelist()
        assert 'altryx_accelerator/config/source_tables.example.json' in archive.namelist()
    with Image.open(ROOT / listing['media']['icon']) as icon:
        assert icon.size == (512,512) and icon.format == 'PNG'
    pdf = ROOT / 'marketplace/assets/README.pdf'
    assert pdf.read_bytes().startswith(b'%PDF')
    samples = sorted((ROOT / 'tests/sample_workflows').glob('*.yxmd'))
    assert len(samples) == 4
    return listing, notebooks, wheel, samples, pdf


def publish(client, catalog, schema, volume, share, notebooks, wheel, samples, pdf):
    from databricks.sdk.errors import NotFound, ResourceAlreadyExists
    from databricks.sdk.service.catalog import VolumeType
    from databricks.sdk.service.sharing import (SharedDataObject, SharedDataObjectDataObjectType as Type,
                                                SharedDataObjectUpdate, SharedDataObjectUpdateAction as Action)
    # Catalog/schema are selected by the provider and must already exist.
    full_name = f'{catalog}.{schema}.{volume}'
    try:
        client.volumes.read(full_name)
    except NotFound:
        try:
            client.volumes.create(catalog_name=catalog, schema_name=schema, name=volume, volume_type=VolumeType.MANAGED)
        except ResourceAlreadyExists:
            pass
    base = f'/Volumes/{catalog}/{schema}/{volume}'
    assets = [(wheel,wheel.name), (pdf,'README.pdf')]
    assets += [(p,'samples/'+p.name) for p in samples]
    assets += [(p,'notebooks/'+p.name) for p in notebooks]
    for directory in ('samples','notebooks'):
        client.files.create_directory(base+'/'+directory)
    for source, relative in assets:
        with source.open('rb') as stream:
            client.files.upload(base+'/'+relative, stream, overwrite=True)
    try:
        current = client.shares.get(share, include_shared_data=True)
    except NotFound:
        try:
            current = client.shares.create(share)
        except ResourceAlreadyExists:
            current = client.shares.get(share, include_shared_data=True)
    existing = {(obj.data_object_type,obj.name) for obj in current.objects or []}
    desired = [SharedDataObject(name=full_name, data_object_type=Type.VOLUME)]
    # Notebook assets carry base64 notebook bytes in the share itself; merely
    # uploading .ipynb files to a Volume does not attach notebook listing assets.
    desired += [SharedDataObject(name=p.name, data_object_type=Type.NOTEBOOK_FILE,
                                content=base64.b64encode(p.read_bytes()).decode()) for p in notebooks]
    updates = []
    for obj in desired:
        present = (obj.data_object_type,obj.name) in existing
        if present and obj.data_object_type == Type.VOLUME:
            continue
        updates.append(SharedDataObjectUpdate(action=Action.UPDATE if present else Action.ADD, data_object=obj))
    if updates:
        client.shares.update(share, updates=updates)
    print(f'Share ready: {share}\nVolume: {base}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--profile', default=os.environ.get('DATABRICKS_CONFIG_PROFILE'))
    for key,default in [('catalog','main'),('schema','marketplace'),('volume','altryx_accelerator'),('share','altryx_accelerator')]:
        parser.add_argument('--'+key, default=os.environ.get('ALTRYX_'+key.upper(),default))
    args = parser.parse_args()
    for key in ('catalog','schema','volume','share'):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',getattr(args,key)):
            parser.error(f'{key} must be a simple UC identifier')
    listing, notebooks, wheel, samples, pdf = validate_assets()
    print(json.dumps(listing, indent=2))
    if args.dry_run:
        print(f'DRY RUN OK: {len(notebooks)} notebooks, {len(samples)} samples, wheel and README.pdf; no workspace calls.')
        return
    from databricks.sdk import WorkspaceClient
    publish(WorkspaceClient(profile=args.profile),args.catalog,args.schema,args.volume,args.share,notebooks,wheel,samples,pdf)
    print('Provider Console → Create listing → Notebooks + Files → select the share → Private Exchange.')

if __name__ == '__main__':
    main()
