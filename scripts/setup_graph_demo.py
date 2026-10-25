"""Install the packaged Graph snapshot into outputs; preserve an active Graph run."""
import argparse
import gzip
import json
from pathlib import Path
import shutil

from pipeline.config import atomic_json
from service.store import BundleStore, Settings


def install(output_root="outputs", snapshot="demo/graph_snapshot"):
    output = Path(output_root)
    if (output/"latest_run.json").exists():
        data, status = BundleStore(Settings(output_root=output, require_graph=True)).load()
        if data is None:
            raise ValueError("Existing selected model is not a valid Graph. Preserve/move old outputs first; no automatic ALS fallback.")
        return {"status":"kept_existing_graph", "model_version":data['model_version']}
    source = Path(snapshot)
    bundle = json.loads((source/"serving/model_bundle.json").read_text())
    destination = output/"runs"/bundle['run_id']
    if destination.exists():
        raise FileExistsError("Destination run exists; inspect it before recovery")
    shutil.copytree(source,destination)
    for archive in (destination/"serving").glob("*.sqlite.gz"):
        with gzip.open(archive,'rb') as src, archive.with_suffix('').open('wb') as dst:
            shutil.copyfileobj(src,dst)
        archive.unlink()
    data,status=BundleStore(Settings(output_root=output,run_id=bundle['run_id'],require_graph=True)).load()
    if data is None:
        raise ValueError(f"Packaged Graph failed validation: {status}")
    atomic_json(output/"latest_run.json", {'run_id':data['run_id'],'model_version':data['model_version']})
    return {'status':'installed','run_id':data['run_id'],'model_version':data['model_version']}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-root',default='outputs')
    p.add_argument('--snapshot',default='demo/graph_snapshot')
    a=p.parse_args()
    print(json.dumps(install(a.output_root,a.snapshot),indent=2))
