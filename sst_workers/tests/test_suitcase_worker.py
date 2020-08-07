import os

import databroker
import databroker.assets.handlers
from event_model import RunRouter
import ophyd.sim
from suitcase import nxsas


def test_nxsas_suitcase(tmp_path):
    nxsas_serializer = nxsas.Serializer(directory=tmp_path, file_prefix="{cycle}-{uid}")

    def nxsas_suitcase_factory(start_doc_name, start_doc):
        return [nxsas_serializer], []

    handler_registry = {
        "AD_TIFF": databroker.assets.handlers.AreaDetectorTiffHandler,
        "NPY_SEQ": ophyd.sim.NumpySeqHandler
    }
    rr = RunRouter([nxsas_suitcase_factory], handler_registry=handler_registry)

    d18_db = databroker.catalog["rsoxs"]["d18"]

    for name, doc in d18_db.canonical(fill="yes"):
        rr(name, doc)

    print(os.listdir(path=tmp_path))
    assert len(os.listdir(path=tmp_path)) == 1
