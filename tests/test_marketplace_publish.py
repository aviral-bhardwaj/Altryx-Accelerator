"""SDK reconciliation is idempotent and dry-run never constructs a client."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from databricks.sdk.errors import NotFound
from databricks.sdk.service.sharing import SharedDataObject, SharedDataObjectDataObjectType as Type

spec = importlib.util.spec_from_file_location('marketplace_publish', Path(__file__).parents[1]/'marketplace/publish.py')
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


def test_publish_add_then_update(tmp_path):
    notebook = tmp_path/'sample.ipynb';notebook.write_text('{"cells":[]}')
    wheel = tmp_path/'package.whl';wheel.write_bytes(b'wheel')
    sample = tmp_path/'sample.yxmd';sample.write_text('<AlteryxDocument/>')
    pdf = tmp_path/'README.pdf';pdf.write_bytes(b'%PDF')
    client = Mock()
    client.volumes.read.side_effect = NotFound('absent')
    client.shares.get.side_effect = NotFound('absent')
    client.shares.create.return_value = SimpleNamespace(objects=[])
    publisher.publish(client,'main','marketplace','accelerator','share',[notebook],wheel,[sample],pdf)
    updates = client.shares.update.call_args.kwargs['updates']
    assert [u.action.value for u in updates] == ['ADD','ADD']
    assert updates[0].data_object.data_object_type == Type.VOLUME
    assert updates[1].data_object.content is not None
    assert client.files.upload.call_count == 4
    client.reset_mock()
    client.volumes.read.side_effect = None
    client.shares.get.side_effect = None
    client.shares.get.return_value = SimpleNamespace(objects=[
        SharedDataObject('main.marketplace.accelerator',data_object_type=Type.VOLUME),
        SharedDataObject('sample.ipynb',data_object_type=Type.NOTEBOOK_FILE)])
    publisher.publish(client,'main','marketplace','accelerator','share',[notebook],wheel,[sample],pdf)
    client.volumes.create.assert_not_called()
    client.shares.create.assert_not_called()
    updates = client.shares.update.call_args.kwargs['updates']
    assert len(updates) == 1 and updates[0].action.value == 'UPDATE'
