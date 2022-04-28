#! /bin/bash
# WARNING : This file is managed by ansible scripts.
# Any changes to this are periodically overwritten.

. /opt/conda/etc/profile.d/conda.sh

# This script defines BS_ENV and BS_PROFILE if they are not already defined
# (presumably in ~/.bashrc or explicitly by the user). If the files does not
# exist _and_ the user has not defines these another way, the script will error
# out violently on `conda activate` below.
if [ -f /etc/bsui/default_vars ]; then
  . /etc/bsui/default_vars
fi


conda_cmd="conda activate $BS_ENV"

$conda_cmd || exit 1

# The --verbose flag was added in bluesky v1.5.0.
# Return 1 if we have at least that version, 0 otherwise.
supports_verbose=`python -c 'import bluesky; from distutils.version import LooseVersion; print(int(LooseVersion(bluesky.__version__) >= LooseVersion("1.5.0")))'`

if [ $supports_verbose -eq 1 ]
then
    flags="--verbose "
else
    flags=""
fi

bluesky-0MQ-proxy $flags 5577 5578