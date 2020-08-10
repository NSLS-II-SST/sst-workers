import logging
import multiprocessing
import os
from pathlib import Path
import time

from bluesky_kafka import Publisher
import databroker
import databroker.assets.handlers
from event_model import RunRouter
import ophyd.sim
import sst_workers.suitcase_worker
from suitcase import nxsas

logging.getLogger(name="bluesky.kafka").setLevel(level=logging.INFO)


def test_nxsas_suitcase(tmp_path):
    nxsas_serializer = nxsas.Serializer(directory=tmp_path, file_prefix="{cycle}-{uid}")

    def nxsas_suitcase_factory(start_doc_name, start_doc):
        return [nxsas_serializer], []

    handler_registry = {
        "AD_TIFF": databroker.assets.handlers.AreaDetectorTiffHandler,
        "NPY_SEQ": ophyd.sim.NumpySeqHandler,
    }
    rr = RunRouter([nxsas_suitcase_factory], handler_registry=handler_registry)

    d18_db = databroker.catalog["rsoxs"]["d18"]

    for name, doc in d18_db.canonical(fill="yes"):
        rr(name, doc)

    print(os.listdir(path=tmp_path))
    assert len(os.listdir(path=tmp_path)) == 1


def test_suitcase_worker(tmp_path):
    # this test passes and then fails

    # start a separate process for the suitcase worker
    multiprocessing.log_to_stderr(level=logging.DEBUG)
    suitcase_worker_process = multiprocessing.Process(
        target=sst_workers.suitcase_worker.start,
        kwargs={
            "export_dir": tmp_path,
            "mongodb_uri": "mongodb://127.0.0.1:27017",
            "kafka_bootstrap_servers": "127.0.0.1:9092",
            "kafka_topics": ["test.rsoxs.bluesky.documents"],
        },
        daemon=True
    )
    suitcase_worker_process.start()
    time.sleep(10)

    kafka_publisher = Publisher(
        topic="test.rsoxs.bluesky.documents",
        key="test_suitcase_worker",
        bootstrap_servers="localhost:9092",
        flush_on_stop_doc=True,
    )
    d18_db = databroker.catalog["rsoxs"]["d18"]
    for name, doc in d18_db.canonical(fill="yes"):
        kafka_publisher(name, doc)

    suitcase_worker_process.terminate()
    suitcase_worker_process.join()

    assert suitcase_worker_process.exitcode == -15

    file_list = os.listdir(path=tmp_path)
    print(file_list)
    file_list = os.listdir(path=tmp_path / Path(file_list[0]))
    print(file_list)
    assert len(file_list) == 1
