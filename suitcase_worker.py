from pathlib import Path

"""
Example:
--------
In [20]: tiff_series.export(db[-3].documents(fill=True), file_prefix='{start[institution]}-Eph={event[data][energy]}-{s
    ...: tart[scan_id]}-', directory=f'Z:/images/users/{hdr.start["user"]}')
Out[20]:
{'stream_data': [WindowsPath('//XF07ID1-WS17/RSoXS Documents/images/users/Eliot/NIST-Eph=459.987181-40-primary-sw_det_saxs_image-0.tiff'),
  WindowsPath('//XF07ID1-WS17/RSoXS Documents/images/users/Eliot/NIST-Eph=459.987181-40-primary-sw_det_waxs_image-0.tiff'),
  WindowsPath('//XF07ID1-WS17/RSoXS Documents/images/users/Eliot/NIST-Eph=459.9903474-40-primary-sw_det_saxs_image-1.tiff'),
  WindowsPath('//XF07ID1-WS17/RSoXS Documents/images/users/Eliot/NIST-Eph=459.9903474-40-primary-sw_det_waxs_image-1.tiff'),
  WindowsPath('//XF07ID1-WS17/RSoXS Documents/images/users/Eliot/NIST-Eph=460.0084854-40-primary-sw_det_saxs_image-2.tiff'),
  WindowsPath('//XF07ID1-WS17/RSoXS Documents/images/users/Eliot/NIST-Eph=460.0084854-40-primary-sw_det_waxs_image-2.tiff')]}
"""
from event_model import RunRouter
from suitcase import tiff_series, csv
import suitcase.jsonl
import datetime
from bluesky_darkframes import DarkSubtraction
from bluesky.callbacks.zmq import RemoteDispatcher
import databroker.assets.handlers

USERDIR = '/DATA/users/'


dispatcher = RemoteDispatcher('localhost:5578')


def factory(name, start_doc):
    dt = datetime.datetime.now()
    formatted_date = dt.strftime('%Y-%m-%d')
    with suitcase.jsonl.Serializer(file_prefix=('{cycle}/'
                                                '{cycle}_'
                                                '{institution}_'
                                                '{user_name}/'
                                                '{project_name}/'
                                                f'{formatted_date}/'
                                                '{scan_id}/'
                                                '{scan_id}-'
                                                '{sample_name}-'
                                                ),
                                   directory=USERDIR,
                                   sort_keys=True,
                                   indent=2) as serializer:
        serializer(name, start_doc)
        # The jsonl Serializer just needs the start doc, so we are done with
        # it now.
    SAXS_sync_subtractor = DarkSubtraction('Synced_saxs_image')
    WAXS_sync_subtractor = DarkSubtraction('Synced_waxs_image')
    SAXS_subtractor = DarkSubtraction('Small Angle CCD Detector_image')
    WAXS_subtractor = DarkSubtraction('Wide Angle CCD Detector_image')
    SWserializer = tiff_series.Serializer(file_prefix=('{start[cycle]}/'
                                                       '{start[cycle]}_'
						       '{start[institution]}_'
						       '{start[user_name]}/'
                                                       '{start[project_name]}/'
                                                       f'{formatted_date}/'
                                                       '{start[scan_id]}/'
                                                       '{start[scan_id]}-'
                                                       '{start[sample_name]}-'
                                                       #'{event[data][en_energy]:.2f}eV-'
                                                       ),
                                          directory=USERDIR)
    name, doc = SWserializer(name, start_doc)
    serializercsv = csv.Serializer(file_prefix=('{start[cycle]}/'
                                                '{start[cycle]}_'
                                                '{start[institution]}_'
                                                '{start[user_name]}/'
                                                '{start[project_name]}/'
                                                f'{formatted_date}/'
                                                '{start[scan_id]}-'
                                                '{start[sample_name]}-'
                                                ),
                                   directory=USERDIR,
                                   flush=True,
                                   line_terminator='\n')

    def fill_subtract_and_serialize(swname, swdoc):
        swname, swdoc = SAXS_sync_subtractor(swname, swdoc)
        swname, swdoc = WAXS_sync_subtractor(swname, swdoc)
        swname, swdoc = SAXS_subtractor(swname, swdoc)
        swname, swdoc = WAXS_subtractor(swname, swdoc)
        SWserializer(swname, swdoc)
    def fill_subtract_and_serialize_saxs(swname, swdoc):
        swname, swdoc = SAXS_sync_subtractor(swname, swdoc)
        swname, swdoc = SAXS_subtractor(swname, swdoc)
        SWserializer(swname, swdoc)
    def fill_subtract_and_serialize_waxs(swname, swdoc):
        swname, swdoc = WAXS_sync_subtractor(swname, swdoc)
        swname, swdoc = WAXS_subtractor(swname, swdoc)
        SWserializer(swname, swdoc)

    def subfactory(dname, descriptor_doc):
        dname, ddoc = dname, descriptor_doc
        if ddoc['name'] in ['primary', 'dark']:
            returnlist = []
            if 'Synced' in start_doc['detectors']:
                name, doc = SAXS_sync_subtractor('start', start_doc)
                WAXS_sync_subtractor(name, doc)
                dname, ddoc = SAXS_sync_subtractor(dname, ddoc)
                dname, ddoc = WAXS_sync_subtractor(dname, ddoc)
                SWserializer(dname, ddoc)
                returnlist.append(fill_subtract_and_serialize)
            elif 'Small Angle CCD Detector' in start_doc['detectors']:
                name, doc = SAXS_subtractor('start', start_doc)
                dname, ddoc = SAXS_subtractor(dname, ddoc)
                SWserializer(dname, ddoc)
                returnlist.append(fill_subtract_and_serialize_saxs)
            elif 'Wide Angle CCD Detector' in start_doc['detectors']:
                name, doc = WAXS_subtractor('start', start_doc)
                dname, ddoc = WAXS_subtractor(dname, ddoc)
                SWserializer(dname, ddoc)
                returnlist.append(fill_subtract_and_serialize_waxs)

            if descriptor_doc['name'] == 'primary':
                serializercsv('start', start_doc)
                serializercsv('descriptor', descriptor_doc)
                returnlist.append(serializercsv)
            return returnlist
        elif 'baseline' in descriptor_doc['name'] or 'monitor' in descriptor_doc['name']:
            dt = datetime.datetime.now()
            formatted_date = dt.strftime('%Y-%m-%d')
            # energy = hdr.table(stream_name='baseline')['Beamline Energy_energy'][1]
            serializer = csv.Serializer(file_prefix=('{start[cycle]}/'
                                                     '{start[cycle]}_'
                                                     '{start[institution]}_'
                                                     '{start[user_name]}/'
                                                     '{start[project_name]}/'
                                                     f'{formatted_date}/'
                                                     '{start[scan_id]}/'
                                                     '{start[scan_id]}-'
                                                     '{start[sample_name]}-'
                                                     #'{event[data][Beamline Energy_energy]:.2f}eV-'
                                                     ),
                                        directory=USERDIR,
                                        flush=True,
                                        line_terminator='\n')
            serializer('start', start_doc)
            serializer('descriptor', descriptor_doc)
            return [serializer]
        else:
            return []

    return [], [subfactory]


import event_model
import suitcase.jsonl


handler_registry = {'AD_TIFF': databroker.assets.handlers.AreaDetectorTiffHandler}
rr = RunRouter([factory], handler_registry=handler_registry)
rr_token = dispatcher.subscribe(rr)
dispatcher.start()