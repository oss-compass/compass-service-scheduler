from director import task, config

import os
import time
import hashlib
import json
import yaml
import configparser
import traceback
import logging
import tldextract
import colorlog

from urllib.parse import urlparse
from os.path import join, exists, abspath
from datetime import datetime

from sirmordred.utils.micro import micro_mordred

from open_metrics_model.metrics_model import ActivityMetricsModel, CommunitySupportMetricsModel

DEFAULT_CONFIG_DIR = 'analysis_data'
CFG_NAME = 'setup.cfg'
CFG_TEMPLATE = 'setup-template.cfg'
JSON_NAME = 'project.json'
SUPPORT_DOMAINS = ['gitee.com', 'github.com']

DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"
INFO_LOG_FORMAT = "%(asctime)s %(message)s"
COLOR_LOG_FORMAT_SUFFIX = "\033[1m %(log_color)s "
LOG_COLORS = {'DEBUG': 'white', 'INFO': 'cyan', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}

# @Task(name='sneakers.analyze_update', queue="analyze_result")
# def update_task(result={}):
#     raise NotImplementedError()

def config_logging(debug, logs_dir, append=True):
    """Config logging level output output"""

    if debug:
        fmt = DEBUG_LOG_FORMAT
        logging_mode = logging.DEBUG
    else:
        fmt = INFO_LOG_FORMAT
        logging_mode = logging.INFO

    # Setting the color scheme and level into the root logger
    logging.basicConfig(level=logging_mode)
    stream = logging.root.handlers[0]
    formatter = colorlog.ColoredFormatter(fmt=COLOR_LOG_FORMAT_SUFFIX + fmt,
                                          log_colors=LOG_COLORS)
    stream.setFormatter(formatter)

    # Creating a file handler and adding it to root
    if logs_dir:
        fh_filepath = os.path.join(logs_dir, 'all.log')
        fh = logging.FileHandler(fh_filepath, mode=('a' if append else 'w'))
        fh.setLevel(logging_mode)
        formatter = logging.Formatter(fmt)
        fh.setFormatter(formatter)
        logging.getLogger().addHandler(fh)

    # ES logger is set to INFO since, it produces a really verbose output if set to DEBUG
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    # Show if debug mode is activated
    if debug:
        logging.debug("Debug mode activated")

# #Example:
# {
#     "raw":true,
#     "enrich":true,
#     "identities_load":false,
#     "identities_merge":false,
#     "panels":false,
#     "metrics_activity":true,
#     "metrics_community":true,
#     "debug":false,
#     "project_url":"https://github.com/manateelazycat/lsp-bridge"
# }

@task(name="etl_v1.extract", queue="analyze_queue_v1")
def extract(*args, **kwargs):
    payload = kwargs['payload']
    project_uri = urlparse(payload['project_url'])
    params = {}
    h = hashlib.new('sha256')
    params['scheme'] = project_uri.scheme
    params['domain'] = project_uri.netloc
    params['path'] = project_uri.path
    params['project_url'] = f"{params['scheme']}://{params['domain']}{params['path']}"
    params['domain_name'] = tldextract.extract(project_uri.netloc).domain
    params['project_key'] = f"{params['domain_name']}{params['path'].replace('/', '-')}"
    h.update(bytes(params['project_url'], encoding='utf-8'))
    params['project_hash'] = h.hexdigest()
    params['raw'] = bool(payload.get('raw'))
    params['identities_load'] = bool(payload.get('identities_load'))
    params['identities_merge'] = bool(payload.get('identities_merge'))
    params['enrich'] = bool(payload.get('enrich'))
    params['panels'] = bool(payload.get('panels'))
    params['debug'] = bool(payload.get('debug'))
    params['metrics_activity'] = bool(payload.get('metrics_activity'))
    params['metrics_community'] = bool(payload.get('metrics_community'))
    if not (params['domain'] in SUPPORT_DOMAINS):
        raise Exception(f"no support project from {payload['project_url']}")
    return params

@task(name="etl_v1.initialize", queue="analyze_queue_v1")
def initialize(*args, **kwargs):
    params = args[0]
    root = config.get('GRIMOIRELAB_CONFIG_FOLDER') or DEFAULT_CONFIG_DIR

    configs_dir = abspath(join(root, params['project_hash'][:2], params['project_hash'][2:]))
    logs_dir = abspath(join(configs_dir, 'logs'))
    metrics_dir = abspath(join(configs_dir, 'metrics'))

    for directory in [configs_dir, logs_dir, metrics_dir]:
        if not exists(directory):
            os.makedirs(directory)

    project_data = {}
    key = params['project_key']
    url = params['project_url']
    domain_name = params['domain_name']

    if domain_name == 'gitee':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][domain_name] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
    elif domain_name == 'github':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][f"{domain_name}:issue"] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]

    project_data_path = join(configs_dir, JSON_NAME)
    with open(project_data_path, 'w') as f:
        json.dump(project_data, f, indent=4, sort_keys=True)

    metrics_data = {}
    metrics_data[key] = {}
    metrics_data[key][domain_name] = [url]

    metrics_data_path = join(metrics_dir, JSON_NAME)
    with open(metrics_data_path, 'w') as jsonfile:
        json.dump(metrics_data, jsonfile, indent=4, sort_keys=True)

    config_logging(params['debug'], logs_dir, False)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    params['project_configs_dir'] = configs_dir
    params['project_logs_dir'] = logs_dir
    params['project_metrics_dir'] = metrics_dir
    params['project_data_path'] = project_data_path
    params['metrics_data_path'] = metrics_data_path

    return params

@task(name="etl_v1.setup", queue="analyze_queue_v1")
def setup(*args, **kwargs):
    params = args[0]

    # create project setup config
    setup = configparser.ConfigParser(allow_no_value=True)
    template_path = config.get('GRIMOIRELAB_CONFIG_TEMPLATE') or CFG_TEMPLATE
    setup.read(template_path)
    setup.set('general', 'logs_dir', params['project_logs_dir'])
    setup.set('projects', 'projects_file', params['project_data_path'])
    setup.set('es_collection', 'url', config.get('ES_URL'))
    setup.set('es_enrichment', 'url', config.get('ES_URL'))

    # default configuration
    backends = ['git']
    domain_name = params['domain_name']
    input_enrich_issues_index = 'github_enriched'
    input_enrich_pulls_index = 'github-pull_enriched'
    if domain_name == 'gitee':
        backends.extend(['gitee', 'gitee:pull', 'gitee:repo'])
        input_enrich_issues_index = 'gitee_issues-enriched'
        input_enrich_pulls_index = 'gitee-prs_enriched'
        api_token = config.get('GITEE_API_TOKEN')
        setup['gitee'] = {
            'raw_index': 'gitee_issues-raw',
            'enriched_index': 'gitee_issues-enriched',
            'category': 'issue',
            'api-token': api_token,
            'sleep-for-rate': 'true',
            'no-archive': 'true',
            'studies': '[enrich_onion:gitee-issue]'
        }

        setup['enrich_onion:gitee-issue'] = {
            'in_index': 'gitee_issues-enriched',
            'out_index': 'gitee_issues_onion-enriched',
            'data_source': 'gitee_issues'
        }

        setup['gitee:pull'] = {
            'raw_index': 'gitee_pulls-raw',
            'enriched_index': 'gitee_pulls-enriched',
            'category': 'pull_request',
            'api-token': api_token,
            'sleep-for-rate': 'true',
            'no-archive': 'true',
            'studies': '[enrich_onion:gitee-pull]'
        }

        setup['enrich_onion:gitee-pull'] = {
            'in_index': 'gitee_pulls-enriched',
            'out_index': 'gitee_pulls_onion-enriched',
            'data_source': 'gitee_pulls'
        }

        setup['gitee:repo'] = {
            'raw_index': 'gitee_repo-raw',
            'enriched_index': 'gitee_repo-enriched',
            'category': 'repository',
            'api-token': api_token,
            'sleep-for-rate': 'true',
            'no-archive': 'true'
        }
    elif domain_name == 'github':
        backends.extend(['github:issue', 'github:pull', 'github:repo'])
        api_token = config.get('GITHUB_API_TOKEN')
        input_enrich_issues_index = 'github_enriched'
        input_enrich_pulls_index = 'github-pull_enriched'
        setup['github:issue'] = {
            'raw_index': 'github_raw',
            'enriched_index': 'github_enriched',
            'category': 'issue',
            'api-token': api_token,
            'sleep-for-rate': 'true',
            'no-archive': 'true',
            'studies': '[enrich_onion:github]'
        }

        setup['enrich_onion:github'] = {
            'in_index_iss': 'github_enriched',
            'in_index_prs': 'github-pull_enriched',
            'out_index_iss': 'github-issues-onion_enriched',
            'out_index_prs': 'github-prs-onion_enriched'
        }

        setup['github:pull'] = {
            'raw_index': 'github-pull_raw',
            'enriched_index': 'github-pull_enriched',
            'category': 'pull_request',
            'api-token': api_token,
            'sleep-for-rate': 'true',
            'no-archive': 'true'
        }

        setup['github:repo'] = {
            'raw_index': 'github-repo_raw',
            'enriched_index': 'gitee_repo-enriched',
            'category': 'repository',
            'api-token': api_token,
            'sleep-for-rate': 'true',
            'no-archive': 'true'
        }
    else:
        pass

    project_setup_path = join(params['project_configs_dir'], CFG_NAME)
    with open(project_setup_path, 'w') as cfg:
        setup.write(cfg)

    params['project_setup_path'] = project_setup_path
    params['project_backends'] = backends
    params['project_issues_index'] = input_enrich_issues_index
    params['project_pulls_index'] = input_enrich_pulls_index
    params['project_git_index'] = 'git_demo_enriched'
    return params

@task(name="etl_v1.raw", queue="analyze_queue_v1", autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def raw(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['raw_started_at'] = datetime.now()
    if params['raw']:
        micro_mordred(
            params['project_setup_path'],
            params['project_backends'],
            None,
            params['raw'],
            False,
            False,
            False,
            False
        )
        params['raw_finished_at'] = datetime.now()
    else:
        params['raw_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.enrich", queue="analyze_queue_v1", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def enrich(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['enrich_started_at'] = datetime.now()
    if params['enrich']:
        micro_mordred(
            params['project_setup_path'],
            params['project_backends'],
            None,
            False,
            False,
            False,
            params['enrich'],
            False
        )
        params['enrich_finished_at'] = datetime.now()
    else:
        params['enrich_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.identities", queue="analyze_queue_v1")
def identities(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['identities_started_at'] = datetime.now()
    if params['identities_load'] or params['identities_merge']:
        micro_mordred(
            params['project_setup_path'],
            params['project_backends'],
            None,
            False,
            params['identities_load'],
            params['identities_merge'],
            False,
            False
        )
        params['identities_finished_at'] = datetime.now()
    else:
        params['identities_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.panels", queue="analyze_queue_v1")
def panels(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['panels_started_at'] = datetime.now()
    if params['panels']:
        micro_mordred(
            params['project_setup_path'],
            params['project_backends'],
            None,
            False,
            False,
            False,
            False,
            params['panels']
        )
        params['panels_finished_at'] = datetime.now()
    else:
        params['panels_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.metrics.activity", queue="analyze_queue_v1", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_activity(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_activity_started_at'] = datetime.now()
    if params['metrics_activity']:
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] =   {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': config.get('METRICS_END_DATE'),
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_activity",
            'community': config.get('METRICS_COMMUNITY'),
            'level': config.get('METRICS_LEVEL')
        }
        params['metrics_activity_params'] = metrics_cfg
        model_activity = ActivityMetricsModel(**metrics_cfg['params'])
        model_activity.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_activity_finished_at'] = datetime.now()
    else:
        params['metrics_activity_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.metrics.community", queue="analyze_queue_v1", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_community(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_community_started_at'] = datetime.now()
    if params['metrics_community']:
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] =   {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': config.get('METRICS_END_DATE'),
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_community",
            'community': config.get('METRICS_COMMUNITY'),
            'level': config.get('METRICS_LEVEL')
        }
        params['metrics_community_params'] = metrics_cfg
        model_community = CommunitySupportMetricsModel(**metrics_cfg['params'])
        model_community.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_community_finished_at'] = datetime.now()
    else:
        params['metrics_community_finished_at'] = 'skipped'
    return params

