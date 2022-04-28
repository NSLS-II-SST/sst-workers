# System Services at SST

### Installing or updating services
`dzdo cp <systemd-unit-service-file> && dzdo systemctl enable ...`

### Current active services
- bluesky-suitcase-worker:
  - Runs suitcase protocol from `suitcase_worker_nsls2data.py`
  - User: softioc-sst
- bluesky-0MQ-proxy
  - User: xf07id1