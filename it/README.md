## Prerequisites
* azure CLI
* Docker

### Start the environment
```bash
source bin/start.sh
```

### Run object storage IO tests
```bash
pytest it_object_storage_io.py -o log_cli=true --log-cli-level=INFO
```

### Stop/cleanup the environment
```bash
bin/stop.sh
```