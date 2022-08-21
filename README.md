
# Compass Service Scheduler

compass-service-scheduler is a task scheduling service based on [celery-director](https://github.com/ovh/celery-director). Used to schedule [CHAOSS Grimoirelab](https://github.com/chaoss/grimoirelab) for data acquisition and metrics model calculation.


## Requirements

1. RabbitMQ 3.6+
2. Redis
3. Mariadb(or MySQL)

## Configuration

Configuration is managed by [environs](https://github.com/sloria/environs).

For details, please refer to the file [.env](./.env)

## Install

```shell
git clone https://github.com/open-metrics-code/compass-service-scheduler

pip install -r requirements.txt
```

## Usage

```shell
export DIRECTOR_HOME=/path/to/compass-service-scheduler

## Web Service UI

director webserver

### or listen to 0.0.0.0

director webserver -b 0.0.0.0:8000

## Celery Worker

director celery worker --loglevel=INFO --queues=analyze_queue_v1 --concurrency 16
```


