config = {
    'metadatastore': {
        'module': 'databroker.headersource.mongo',
        'class': 'MDS',
        'config': {
            'directory': 'some_directory',
            'timezone': 'US/Eastern'}
    },
    'assets': {
        'module': 'databroker.assets.sqlite',
        'class': 'Registry',
        'config': {
            'dbpath': assets_dir + '/database.sql'}
    }
}


