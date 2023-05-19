from director import task, config

import os
import json

from os.path import join, exists, abspath
from urllib.parse import urlparse
from datetime import datetime

from . import config_logging
from ..utils import tools

from compass_metrics_model.metrics_model import (
    ActivityMetricsModel,
    CommunitySupportMetricsModel,
    CodeQualityGuaranteeMetricsModel,
    OrganizationsActivityMetricsModel
)

DEFAULT_CONFIG_DIR = 'custom_data'
JSON_NAME = 'project.json'

@task(name="custom_v1.extract", bind=True)
def extract(self, *args, **kwargs):
    payload = kwargs['payload']
    url = payload['project_url']
    level = payload.get('level')
    params = {}
    params['scheme'], params['domain'], params['path'] = tools.extract_url_info(url)
    params['project_url'] = tools.normalize_url(url)
    params['domain_name'] = tools.extract_domain(url)
    params['project_key'] = tools.normalize_key(url)
    params['project_hash'] = tools.hash_string(params['project_url'])
    params['level'] = ('community' if level == 'project' or level == 'community' else 'repo')
    params['debug'] = bool(payload.get('debug'))
    params['metrics_activity'] = bool(payload.get('metrics_activity'))
    params['metrics_community'] = bool(payload.get('metrics_community'))
    params['metrics_codequality'] = bool(payload.get('metrics_codequality'))
    params['metrics_group_activity'] = bool(payload.get('metrics_group_activity'))
    params['weights'] = payload.get('metrics_weights')
    params['custom_fields'] = payload.get('metrics_custom_fields')

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

    project_data = {}
    key = params['project_key']
    url = params['project_url']
    domain_name = params['domain_name']
    project_data = tools.gen_project_section(project_data, domain_name, key, url)

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


@task(name="custom_v1.setup")
def setup(*args, **kwargs):
    params = args[0]
    domain_name = params['domain_name']

    params['project_issues_index'] = f"{domain_name}-issues_enriched"
    params['project_issues2_index'] = f"{domain_name}2-issues_enriched"
    params['project_pulls_index'] = f"{domain_name}-pulls_enriched"
    params['project_pulls2_index'] = f"{domain_name}2-pulls_enriched"
    params['project_git_index'] = f"{domain_name}-git_enriched"
    params['project_repo_index'] = f"{domain_name}-repo_enriched"
    params['project_release_index'] = f"{domain_name}-releases_enriched"
    params['project_contributors_index'] = f"{domain_name}-contributors_org_repo"

    return params


@task(name="custom_v1.metrics.activity", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 0})
def metrics_activity(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_activity_started_at'] = datetime.now()

    if params.get('metrics_activity'):
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'issue_index': params['project_issues_index'],
            'repo_index': params['project_repo_index'],
            'pr_index': params['project_pulls_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_activity_custom",
            'git_branch': None,
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'weights': params['weights'],
            'custom_fields': params['custom_fields'],
            'release_index': params['project_release_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index']
        }
        params["metrics_activity_params"] = metrics_cfg
        model_activity = ActivityMetricsModel(**metrics_cfg['params'])
        model_activity.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_activity_finished_at'] = datetime.now()
    else:
        params['metrics_activity_finished_at'] = 'skipped'
    return params


@task(name="custom_v1.metrics.community", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 0})
def metrics_community(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_community_started_at'] = datetime.now()

    if params.get('metrics_community'):
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'git_index': params['project_git_index'],
            'json_file': params['metrics_data_path'],
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_community_custom",
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'weights': params['weights'],
            'custom_fields': params['custom_fields']
        }
        params["metrics_community_params"] = metrics_cfg
        model_community = CommunitySupportMetricsModel(**metrics_cfg['params'])
        model_community.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_community_finished_at'] = datetime.now()
    else:
        params['metrics_community_finished_at'] = 'skipped'
    return params


@task(name="custom_v1.metrics.codequality", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 0})
def metrics_codequality(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_codequality_started_at'] = datetime.now()

    if params.get('metrics_codequality'):
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'repo_index': params['project_repo_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_codequality_custom",
            'git_branch': None,
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'weights': params['weights'],
            'custom_fields': params['custom_fields'],
            'company': None,
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index']
        }
        params["metrics_codequality_params"] = metrics_cfg
        model_codequality = CodeQualityGuaranteeMetricsModel(**metrics_cfg['params'])
        model_codequality.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_codequality_finished_at'] = datetime.now()
    else:
        params['metrics_codequality_finished_at'] = 'skipped'
    return params


@task(name="custom_v1.metrics.group_activity", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 0})
def metrics_group_activity(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_group_activity_started_at'] = datetime.now()

    if params.get('metrics_group_activity'):
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'issue_index': params['project_issues_index'],
            'repo_index': params['project_repo_index'],
            'pr_index': params['project_pulls_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_group_activity_custom",
            'git_branch': None,
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'weights': params['weights'],
            'custom_fields': params['custom_fields'],
            'company': None,
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index']
        }
        params[f"metrics_group_activity_params"] = metrics_cfg
        model_codequality = OrganizationsActivityMetricsModel(**metrics_cfg['params'])
        model_codequality.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_group_activity_finished_at'] = datetime.now()
    else:
        params['metrics_group_activity_finished_at'] = 'skipped'
    return params
