from bluesky_kafka import Publisher
import databroker.assets.handlers
from event_model import RunRouter
import ophyd.sim


kafka_publisher = Publisher(
    topic="test.rsoxs.bluesky.documents",
    key="test_suitcase_worker",
    bootstrap_servers="localhost:9092",
    flush_on_stop_doc=True,
)


def kafka_publisher_factory(start_doc_name, start_doc):
    return [kafka_publisher], []


rr = RunRouter(
    [kafka_publisher_factory],
    handler_registry={
        "AD_TIFF": databroker.assets.handlers.AreaDetectorTiffHandler,
        "NPY_SEQ": ophyd.sim.NumpySeqHandler,
    },
)

d18_db = databroker.catalog["rsoxs"]["d18"]

for name, doc in d18_db.canonical(fill="yes"):
    print(f"name: {name}")
    rr(name, doc)
