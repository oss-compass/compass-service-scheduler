from director import task, config

import os
import json
import configparser

from os.path import join, exists, abspath
from urllib.parse import urlparse
from datetime import datetime, timedelta

from compass_model.base_metrics_model import BaseMetricsModel

from . import config_logging
from ..utils import tools

DEFAULT_CONFIG_DIR = 'custom_data'
JSON_NAME = 'project.json'
CFG_TEMPLATE = 'setup-template.cfg'

@task(name="custom_v1.extract", bind=True)
def extract(self, *args, **kwargs):
    payload = kwargs['payload']
    urls = payload['project_urls']
    level = payload.get('level')

    params = {
        'dataset': {},
        'level': ('community' if level == 'project' or level == 'community' else 'repo'),
        'metrics_weights_thresholds': payload.get('metrics_weights_thresholds'),
        'custom_fields': payload.get('custom_fields'),
        'algorithm': payload.get('algorithm') or 'criticality_score',
        'debug': bool(payload.get('debug'))
    }

    for url in urls:
        label = tools.normalize_url(url)
        data = {}
        data['project_url'] = label
        data['domain_name'] = tools.extract_domain(label)
        data['project_key'] = tools.normalize_key(label)
        params['dataset'][label] = data

    params['project_hash'] = tools.hash_string(','.join(params['dataset'].keys()))

    return params

@task(name="custom_v1.initialize")
def initialize(*args, **kwargs):
    params = args[0]
    root = config.get('COMPASS_CUSTOM_CONFIG_FOLDER') or DEFAULT_CONFIG_DIR

    configs_dir = abspath(join(root, params['project_hash'][:2], params['project_hash'][2:]))
    logs_dir = abspath(join(configs_dir, 'logs'))
    metrics_dir = abspath(join(configs_dir, 'metrics'))

    for directory in [configs_dir, logs_dir, metrics_dir]:
        if not exists(directory):
            os.makedirs(directory)

    dataset = params['dataset']
    project_data = {}

    for label, data in dataset.items():
        key = data['project_key']
        url = data['project_url']
        domain_name = data['domain_name']
        project_data = tools.gen_project_section(project_data, domain_name, key, url)

        metrics_data = {}
        metrics_data[key] = {}
        metrics_data[key][domain_name] = [url]
        metrics_data_path = join(metrics_dir, f"{key}.json")

        with open(metrics_data_path, 'w') as jsonfile:
            json.dump(metrics_data, jsonfile, indent=4, sort_keys=True)

    project_data_path = join(configs_dir, JSON_NAME)
    with open(project_data_path, 'w') as f:
        json.dump(project_data, f, indent=4, sort_keys=True)


    config_logging(params['debug'], logs_dir, False)

    params['project_configs_dir'] = configs_dir
    params['project_logs_dir'] = logs_dir
    params['project_metrics_dir'] = metrics_dir
    params['project_data_path'] = project_data_path
    return params

@task(name="custom_v1.setup")
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

    dataset = params['dataset']

    for label, data in dataset.items():
        key = data['project_key']
        url = data['project_url']
        domain_name = data['domain_name']
        # default configuration
        backends = []

        input_event_raw_index = f"{domain_name}-event_raw"
        input_event_enriched_index = f"{domain_name}-event_enriched"

        input_stargazer_raw_index = f"{domain_name}-stargazer_raw"
        input_stargazer_enriched_index = f"{domain_name}-stargazer_enriched"

        input_fork_raw_index = f"{domain_name}-fork_raw"
        input_fork_enriched_index = f"{domain_name}-fork_enriched"

        event_cfg = {
            'raw_index': input_event_raw_index,
            'enriched_index': input_event_enriched_index,
            'category': 'event',
            'sleep-for-rate': 'true',
            'no-archive': 'true'
        }

        fork_cfg = {
            'raw_index': input_fork_raw_index,
            'enriched_index': input_fork_enriched_index,
            'category': 'fork',
            'sleep-for-rate': 'true',
            'no-archive': 'true'
        }

        stargazer_cfg = {
            'raw_index': input_stargazer_raw_index,
            'enriched_index': input_stargazer_enriched_index,
            'category': 'stargazer',
            'sleep-for-rate': 'true',
            'no-archive': 'true'
        }
        if domain_name == 'github':
            backends.extend(['githubql:event', 'githubql:stargazer', 'githubql:fork'])
            extra = {'api-token': config.get('GITEE_API_TOKEN')}
            setup['githubql:event'] = {**event_cfg, **extra}
            setup['githubql:stargazer'] = {**stargazer_cfg, **extra}
            setup['githubql:fork'] = {**fork_cfg, **extra}
        else:
            pass

        project_setup_path = join(params['project_configs_dir'], f"{key}.cfg")
        with open(project_setup_path, 'w') as cfg:
            setup.write(cfg)

    return params


@task(name="custom_v1.metrics.caculate", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def caculate(*args, **kwargs):
    params = args[0]

    config_logging(params['debug'], params['project_logs_dir'])

    for label, data in params['dataset'].items():
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': f"{data['domain_name']}-repo_enriched",
            'git_index': f"{data['domain_name']}-git_enriched",
            'issue_index': f"{data['domain_name']}-issues_enriched",
            'pr_index': f"{data['domain_name']}-pulls_enriched",
            'issue_comments_index': f"{data['domain_name']}2-issues_enriched",
            'pr_comments_index': f"{data['domain_name']}2-pulls_enriched",
            'contributors_index': f"{data['domain_name']}-contributors_org_repo",
            'release_index': f"{data['domain_name']}-releases_enriched",
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_custom_v1",
            'from_date': (datetime.now() - timedelta(days=30 * 6)).strftime('%Y-%m-%d'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'level': params['level'],
            'community': data['project_key'],
            'source': data['domain_name'],
            'json_file': join(params['project_metrics_dir'], f"{data['project_key']}.json"),
            'model_name': f"custom_v1",
            'metrics_weights_thresholds': params['metrics_weights_thresholds'],
            'algorithm': params['algorithm'],
            'custom_fields': params['custom_fields']
        }
        params[f"{data['project_key']}_custom_metrics_started_at"] = datetime.now()
        params[f"{data['project_key']}_custom_metrics_params"] = metrics_cfg
        custom_model = BaseMetricsModel(**metrics_cfg['params'])
        custom_model.metrics_model_metrics(metrics_cfg['url'])
        params[f"{data['project_key']}_custom_metrics_finished_at"] = datetime.now()
    return params
