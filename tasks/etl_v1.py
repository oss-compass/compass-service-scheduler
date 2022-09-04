from director import task, config

import os
import time
import hashlib
import json
import yaml
import configparser

import tldextract
import requests

from urllib.parse import urlparse
from os.path import join, exists, abspath
from datetime import datetime

from . import config_logging
from sirmordred.utils.micro import micro_mordred

from compass_metrics_model.metrics_model import ActivityMetricsModel, CommunitySupportMetricsModel, CodeQualityGuaranteeMetricsModel

DEFAULT_CONFIG_DIR = 'analysis_data'
CFG_NAME = 'setup.cfg'
CFG_TEMPLATE = 'setup-template.cfg'
JSON_NAME = 'project.json'
SUPPORT_DOMAINS = ['gitee.com', 'github.com', 'raw.githubusercontent.com']

def normalize_url(url):
    uri = urlparse(url)
    return f"{uri.scheme}://{uri.netloc}{uri.path}"

def normalize_key(url):
    uri = urlparse(url)
    domain_name = tldextract.extract(uri.netloc).domain
    return f"{domain_name}{uri.path.replace('/', '-')}"

def gen_project_section(project_data, domain_name, key, url):
    if domain_name == 'gitee':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][domain_name] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
    elif domain_name == 'github':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][f"{domain_name}:issue"] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
    return project_data

def load_yaml_template(url):
    proxies = {
        'http': config.get('GITHUB_PROXY'),
        'https': config.get('GITHUB_PROXY'),
    }
    uri = urlparse(url)
    domain_name = tldextract.extract(uri.netloc).domain
    if domain_name == 'gitee':
        return yaml.safe_load(requests.get(url, allow_redirects=True).text)
    else:
        return yaml.safe_load(requests.get(url, allow_redirects=True, proxies=proxies).text)
# #Repository Example:
# {
#     "raw":true,
#     "enrich":true,
#     "identities_load":false,
#     "identities_merge":false,
#     "panels":false,
#     "metrics_activity":true,
#     "metrics_community":true,
#     "metrics_codequality":true,
#     "debug":false,
#     "project_url":"https://github.com/manateelazycat/lsp-bridge",
#     "level":"repo"
# }


# #Project Example:
# {
#     "raw":true,
#     "enrich":true,
#     "identities_load":false,
#     "identities_merge":false,
#     "panels":false,
#     "metrics_activity":true,
#     "metrics_community":true,
#     "metrics_codequality":true,
#     "debug":false,
#     "project_template_yaml":"https://gitee.com/edmondfrank/compass-project-template/raw/master/mindspore/mindspores.yaml",
#     "level":"project"
# }

@task(name="etl_v1.extract")
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
    params['level'] = ('project' if payload.get('level') == 'project' else 'repo')
    params['debug'] = bool(payload.get('debug'))
    params['metrics_activity'] = bool(payload.get('metrics_activity'))
    params['metrics_community'] = bool(payload.get('metrics_community'))
    params['metrics_codequality'] = bool(payload.get('metrics_codequality'))
    if not (params['domain'] in SUPPORT_DOMAINS):
        raise Exception(f"no support project from {project_uri}")
    return params

@task(name="etl_v1.extract_group")
def extract_group(*args, **kwargs):
    payload = kwargs['payload']
    project_yaml = urlparse(payload['project_template_yaml'])
    params = {}
    h = hashlib.new('sha256')
    params['scheme'] = project_yaml.scheme
    params['domain'] = project_yaml.netloc
    params['path'] = project_yaml.path
    if not (params['domain'] in SUPPORT_DOMAINS):
        raise Exception(f"no support project from {project_yaml}")
    project_yaml_url = f"{params['scheme']}://{params['domain']}{params['path']}"
    params['project_yaml_url'] = project_yaml_url
    params['project_yaml'] = load_yaml_template(project_yaml_url)
    params['project_key'] = next(iter(params['project_yaml']))
    params['project_urls'] = params['project_yaml'].get(params['project_key']).get('repositories')
    params['domain_name'] = 'gitee' if tldextract.extract(project_yaml.netloc).domain == 'gitee' else 'github'
    h.update(bytes(params['path'], encoding='utf-8'))
    params['project_hash'] = h.hexdigest()
    params['raw'] = bool(payload.get('raw'))
    params['identities_load'] = bool(payload.get('identities_load'))
    params['identities_merge'] = bool(payload.get('identities_merge'))
    params['enrich'] = bool(payload.get('enrich'))
    params['panels'] = bool(payload.get('panels'))
    params['level'] = ('project' if payload.get('level') == 'project' else 'repo')
    params['debug'] = bool(payload.get('debug'))
    params['metrics_activity'] = bool(payload.get('metrics_activity'))
    params['metrics_community'] = bool(payload.get('metrics_community'))
    params['metrics_codequality'] = bool(payload.get('metrics_codequality'))
    return params

@task(name="etl_v1.initialize")
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
    project_data = gen_project_section(project_data, domain_name, key, url)

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

    params['project_configs_dir'] = configs_dir
    params['project_logs_dir'] = logs_dir
    params['project_metrics_dir'] = metrics_dir
    params['project_data_path'] = project_data_path
    params['metrics_data_path'] = metrics_data_path

    return params

@task(name="etl_v1.initialize_group")
def initialize_group(*args, **kwargs):
    params = args[0]
    root = config.get('GRIMOIRELAB_CONFIG_FOLDER') or DEFAULT_CONFIG_DIR

    configs_dir = abspath(join(root, params['project_hash'][:2], params['project_hash'][2:]))
    logs_dir = abspath(join(configs_dir, 'logs'))
    metrics_dir = abspath(join(configs_dir, 'metrics'))

    for directory in [configs_dir, logs_dir, metrics_dir]:
        if not exists(directory):
            os.makedirs(directory)

    project_data = {}
    name = params['project_key']
    urls = params['project_urls']
    domain_name = params['domain_name']

    for project_url in urls:
        url = normalize_url(project_url)
        key = normalize_key(project_url)
        project_data = gen_project_section(project_data, domain_name, key, url)

    project_data_path = join(configs_dir, JSON_NAME)
    with open(project_data_path, 'w') as f:
        json.dump(project_data, f, indent=4, sort_keys=True)

    metrics_data = {}
    metrics_data[name] = {}
    metrics_data[name][domain_name] = urls

    metrics_data_path = join(metrics_dir, JSON_NAME)
    with open(metrics_data_path, 'w') as jsonfile:
        json.dump(metrics_data, jsonfile, indent=4, sort_keys=True)

    config_logging(params['debug'], logs_dir, False)

    params['project_configs_dir'] = configs_dir
    params['project_logs_dir'] = logs_dir
    params['project_metrics_dir'] = metrics_dir
    params['project_data_path'] = project_data_path
    params['metrics_data_path'] = metrics_data_path

    return params

@task(name="etl_v1.setup")
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
    project_key = params['project_key']
    domain_name = params['domain_name']

    input_git_raw_index = f"{project_key}_raw"
    input_git_enriched_index = f"{project_key}_enriched"

    input_repo_raw_index = f"{project_key}-repo_raw"
    input_repo_enriched_index = f"{project_key}-repo_enriched"

    input_raw_issues_index = f"{project_key}-issues_raw"
    input_enrich_issues_index = f"{project_key}-issues_enriched"

    input_raw_issues2_index = f"{project_key}2-issues_raw"
    input_enrich_issues2_index = f"{project_key}2-issues_enriched"

    input_raw_pulls_index = f"{project_key}-pulls_raw"
    input_enrich_pulls_index = f"{project_key}-pulls_enriched"

    input_raw_pulls2_index = f"{project_key}2-pulls_raw"
    input_enrich_pulls2_index = f"{project_key}2-pulls_enriched"

    input_enrich_releases_index = f"{project_key}-releases_enriched"

    setup['git'] = {
        'raw_index': input_git_raw_index,
        'enriched_index': input_git_enriched_index,
        'latest-items': 'false',
        'category': 'commit'
    }

    issues_cfg = {
        'raw_index': input_raw_issues_index,
        'enriched_index': input_enrich_issues_index,
        'category': 'issue',
        'sleep-for-rate': 'true',
        'no-archive': 'true'
    }

    issues2_cfg = {
        'raw_index': input_raw_issues2_index,
        'enriched_index': input_enrich_issues2_index,
        'category': 'issue',
        'sleep-for-rate': 'true',
        'no-archive': 'true'
    }

    pulls_cfg = {
        'raw_index': input_raw_pulls_index,
        'enriched_index': input_enrich_pulls_index,
        'category': 'pull_request',
        'sleep-for-rate': 'true',
        'no-archive': 'true'
    }

    pulls2_cfg = {
        'raw_index': input_raw_pulls2_index,
        'enriched_index': input_enrich_pulls2_index,
        'category': 'pull_request',
        'sleep-for-rate': 'true',
        'no-archive': 'true'
    }

    repo_cfg = {
        'raw_index': input_repo_raw_index,
        'enriched_index': input_repo_enriched_index,
        'category': 'repository',
        'sleep-for-rate': 'true',
        'no-archive': 'true'
    }

    if domain_name == 'gitee':
        backends.extend(['gitee', 'gitee:pull', 'gitee:repo', 'gitee2:issue', 'gitee2:pull'])
        extra = {'api-token': config.get('GITEE_API_TOKEN')}
        setup['gitee'] = {**issues_cfg, **extra}
        setup['gitee2:issue'] = {**issues2_cfg, **extra}
        setup['gitee:pull'] = {**pulls_cfg, **extra}
        setup['gitee2:pull'] = {**pulls2_cfg, **extra}
        setup['gitee:repo'] = {**repo_cfg, **extra}
    elif domain_name == 'github':
        backends.extend(['github:issue', 'github:pull', 'github:repo', 'github2:issue', 'github2:pull'])
        extra = {'proxy': config.get('GITHUB_PROXY'), 'api-token': config.get('GITHUB_API_TOKEN')}
        setup['github:issue'] = {**issues_cfg, **extra}
        setup['github2:issue'] = {**issues2_cfg, **extra}
        setup['github:pull'] = {**pulls_cfg, **extra}
        setup['github2:pull'] = {**pulls2_cfg, **extra}
        setup['github:repo'] = {**repo_cfg, **extra}
    else:
        pass

    project_setup_path = join(params['project_configs_dir'], CFG_NAME)
    with open(project_setup_path, 'w') as cfg:
        setup.write(cfg)

    params['project_setup_path'] = project_setup_path
    params['project_backends'] = backends
    params['project_issues_index'] = input_enrich_issues_index
    params['project_pulls_index'] = input_enrich_pulls_index
    params['project_pulls2_index'] = input_enrich_pulls2_index
    params['project_git_index'] = input_git_enriched_index
    params['project_release_index'] = input_enrich_releases_index
    return params

@task(name="etl_v1.raw", autoretry_for=(Exception,), retry_kwargs={'max_retries': 5}, acks_late=True)
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

@task(name="etl_v1.enrich", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3}, acks_late=True)
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

@task(name="etl_v1.identities", acks_late=True)
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

@task(name="etl_v1.panels", acks_late=True)
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

@task(name="etl_v1.metrics.activity", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_activity(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_activity_started_at'] = datetime.now()
    if params['metrics_activity']:
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] =   {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'release_index': params['project_release_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'out_index': f"{project_key}_activity",
            'community': project_key,
            'level': params['level']
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
    project_key = params['project_key']
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
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'out_index': f"{project_key}_community",
            'community': project_key,
            'level': params['level']
        }
        params['metrics_community_params'] = metrics_cfg
        model_community = CommunitySupportMetricsModel(**metrics_cfg['params'])
        model_community.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_community_finished_at'] = datetime.now()
    else:
        params['metrics_community_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.metrics.codequality", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_codequality(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_codequality_started_at'] = datetime.now()
    if params['metrics_codequality']:
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] =   {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'out_index': f"{project_key}_codequality",
            'community': project_key,
            'level': params['level'],
            'company': None,
            'pr_comments_index': params['project_pulls2_index']
        }
        params['metrics_codequality_params'] = metrics_cfg
        model_codequality = CodeQualityGuaranteeMetricsModel(**metrics_cfg['params'])
        model_codequality.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_codequality_finished_at'] = datetime.now()
    else:
        params['metrics_codequality_finished_at'] = 'skipped'
    return params

