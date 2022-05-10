# System Services at SST

### Installing or updating services
Currently copying to `/etc` requires root access.
`dzdo cp <systemd-unit-service-file>  /etc/systemd/system && dzdo systemctl enable ...`

### Current active services
- bluesky-suitcase-worker:
  - Runs suitcase protocol from `suitcase_worker_nsls2data.py`
  - User: softioc-sst
- bluesky-0MQ-proxy
  - Runs bash script `run-default-bluesky-0MQ-proxy.sh` in usr bin.
  - User: xf07id1

### To start a service
`dzdo systemctl start <NAME>`, where `<NAME>` is the service from the above list. 