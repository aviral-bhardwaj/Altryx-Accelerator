# Publish Altryx Accelerator 1.1.0

This is a **Notebooks + Files** Marketplace product. It is not a solution-accelerator listing type or a partner-listed Databricks App. Consumers receive assets in a Delta Sharing share, not this Git repository. The share contains four notebook assets (below the ten-notebook limit) and a Unity Catalog Volume containing the wheel, four sample workflows, notebook copies and README.pdf.

## Prepare and validate locally

```bash
python -m pip install '.[dev]'
SOURCE_DATE_EPOCH=1793232000 python -m build
python marketplace/build_notebooks.py
python marketplace/publish.py --dry-run
```

`--dry-run` validates versions, notebook format, package data, release fallback URLs, the four samples, icon and PDF without constructing a workspace client. It does not verify workspace permissions, private-exchange membership, privacy-page availability or Databricks Runtime execution.

## Publish first to a Private Exchange

An account administrator must enable the appropriate Marketplace provider and Unity Catalog capabilities. Configure a provider profile and Marketplace admin permissions. Public publication requires Premium, Unity Catalog, account-admin setup and approval through the Databricks Data Partner Program. Private providers follow the private-provider onboarding flow.

Use an existing UC catalog and schema with managed storage; the publishing identity needs USE CATALOG, USE SCHEMA, CREATE VOLUME, read/write volume permissions and permission to create/update shares. Authenticate using the CLI profile or Databricks environment credentials. Never put credentials into this repository.

```bash
databricks auth login --profile marketplace
python marketplace/publish.py --profile marketplace \
  --catalog main --schema marketplace --volume altryx_accelerator \
  --share altryx_accelerator
```

The script creates the managed Volume if absent, overwrites the versioned release assets, creates the share if absent, adds the Volume and adds/updates all four NOTEBOOK_FILE assets. It preserves unrelated share assets. Configure equivalent defaults with ALTRYX_CATALOG, ALTRYX_SCHEMA, ALTRYX_VOLUME and ALTRYX_SHARE. The catalog and schema must exist before running.

1. Open **Marketplace → Provider console → Private exchanges**. Create an exchange and invite a separate test consumer account.
2. Select **Listings → Create listing**. Copy the fields printed by the script from `listing.yaml`.
3. Choose **Notebooks** and **Files**, then select the `altryx_accelerator` share.
4. Select the private exchange as the audience. Attach `marketplace/assets/icon.png`, verify the public privacy-policy page exists, review and publish privately.
5. As the test consumer, get the product, install its shared catalog and import the four notebooks. Set `wheel_path` to the consumer's actual `/Volumes/<shared_catalog>/<schema>/altryx_accelerator/altryx_accelerator-1.1.0-py3-none-any.whl`, and `samples_dir` to the sibling `samples` folder.
6. Attach a UC-enabled cluster, Run All in notebook 02, then run 01 with your own workflow. For external inputs use 03 to map input tool IDs/annotations/path fragments to UC tables. Verify output values against your Alteryx baseline before production use.
7. Once consumer acceptance passes and public-provider approval is complete, use Provider Console to submit the listing for public review.

Notebook 04 is also the Asset Bundle job entry point. The job's ipynb-only override avoids Workspace notebook-name collisions; interactive and CLI defaults emit both ipynb and py. The bundle developer workflow is optional and is not delivered through the listing.

## Installation and offline behavior

Every notebook starts with wheel_path/pip_fallback widgets and notebook-scoped `%pip` installation, followed by `dbutils.library.restartPython()`. All subsequent cells re-import state and read persisted widgets. The release URL is:

`https://github.com/aviral-bhardwaj/Altryx-Accelerator/releases/download/v1.1.0/altryx_accelerator-1.1.0-py3-none-any.whl`

A hidden input cell embeds the built wheel as a base64 zip if it is smaller than 300 KB. This offline fallback installs without downloading dependencies: lxml, PyYAML and nbformat must already be available. For a fully disconnected cluster, install those dependencies from your organization's approved wheelhouse first. A volume wheel takes precedence, then the GitHub release, then the embedded wheel. Rebuild the embedded payload after any package change.

The release workflow attaches the wheel on tag `v1.1.0`; before that tag completes, the Volume wheel or embedded fallback is required. Publishing a share does not create a Marketplace listing automatically: audience and commercial/provider details are finalized in Provider Console.

## References

- [Create a Marketplace listing](https://docs.databricks.com/aws/en/marketplace/create-listing)
- [Become a Marketplace provider](https://docs.databricks.com/aws/en/marketplace/get-started-provider)
- [Private exchanges](https://docs.databricks.com/aws/en/marketplace/private-exchange)
- [SDK share objects: NOTEBOOK_FILE content and VOLUME](https://databricks-sdk-py.readthedocs.io/en/stable/dbdataclasses/sharing.html)

Partner-published Databricks Apps are not currently a Marketplace delivery option; this release uses supported notebook and file assets.
