from director import task, config

import os
import time
import json
import configparser
import requests
import urllib.parse

from os.path import join, exists, abspath
from urllib.parse import urlparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from . import config_logging
from ..utils import tools
from sirmordred.utils.micro import micro_mordred

from elasticsearch import Elasticsearch, RequestsHttpConnection

from compass_metrics_model.metrics_model import (
    ActivityMetricsModel,
    CommunitySupportMetricsModel,
    CodeQualityGuaranteeMetricsModel,
    OrganizationsActivityMetricsModel
)

# New Metrics Model

from compass_model.contributor.productivity.domain_persona_metrics_model import DomainPersonaMetricsModel
from compass_model.contributor.productivity.milestone_persona_metrics_model import MilestonePersonaMetricsModel
from compass_model.contributor.productivity.role_persona_metrics_model import RolePersonaMetricsModel
from compass_model.software_artifact.robustness.criticality_score_metrics_model import CriticalityScoreMetricsModel 
from compass_model.software_artifact.robustness.scorecard_metrics_model import ScorecardMetricsModel 

from compass_contributor.contributor_dev_org_repo import ContributorDevOrgRepo
from compass_metrics_model.metrics_model_custom import MetricsModelCustom


DEFAULT_CONFIG_DIR = 'analysis_data'
CFG_NAME = 'setup.cfg'
CFG_TEMPLATE = 'setup-template.cfg'
JSON_NAME = 'project.json'
SUPPORT_DOMAINS = ['gitee.com', 'github.com', 'raw.githubusercontent.com', 'gitcode.com']


def validate_callback(callback):
    if type(callback) == dict and 'hook_url' in callback and \
       'params' in callback:
        if callback['hook_url'] and callback['params'] and \
           type(callback['params']) == dict and \
           type(callback['hook_url']) == str:
            return True
    return False

# #Repository Example:
# {
#     "opencheck_raw":true,
#     "opencheck_raw_param":{
#         "commands": ["binary-checker","scancode"],
#         "access_token": "access_token"
#     },
#     "raw":true,
#     "enrich":true,
#     "raw_enrich_setup": ["git","issue", "issue2", "pull", "pull2", "repo", "stargazer", "fork", "watch", "event"],
#     "identities_load":true,
#     "identities_merge":true,
#     "panels":false,
#     "metrics_activity":true,
#     "metrics_community":true,
#     "metrics_codequality":true,
#     "metrics_group_activity":true,
#     "metrics_domain_persona":true,
#     "metrics_milestone_persona":true,
#     "metrics_role_persona":true,
#     "metrics_criticality_score":true,
#     "metrics_scorecard":true,
#     "debug":false,
#     "project_url":"https://github.com/manateelazycat/lsp-bridge",
#     "level":"repo",
#     "callback": {
#       "hook_url": "http://106.13.250.196:3000/api/hook",
#       "params": {},
#     },
#     "from-date": "2000-01-01",
#     "to-date": "2099-01-01",
# }

# #Project Example:
# {
#     "raw":true,
#     "enrich":true,
#     "identities_load":true,
#     "identities_merge":true,
#     "panels":false,
#     "metrics_activity":true,
#     "metrics_community":true,
#     "metrics_codequality":true,
#     "metrics_group_activity":true,
#     "metrics_domain_persona":true,
#     "metrics_milestone_persona":true,
#     "metrics_role_persona":true,
#     "debug":false,
#     "project_template_yaml":"https://gitee.com/edmondfrank/compass-project-template/raw/main/organizations/EAF.yml",
#     "level":"community",
#     "callback": {
#       "hook_url": "http://106.13.250.196:3000/api/hook",
#       "params": {},
#     }
# }

@task(name="etl_v1.extract", bind=True)
def extract(self, *args, **kwargs):
    payload = kwargs['payload']
    url = payload['project_url']
    level = payload.get('level')
    params = {}
    params['scheme'], params['domain'], params['path'] = tools.extract_url_info(url)
    params['callback'] = callback = payload.get('callback')
    if not (params['domain'] in SUPPORT_DOMAINS):
        message = f"no support project from {url}"
        if validate_callback(callback):
            if callback['params'].get("callback_type", "") != "tpc_software_callback":
                callback['params']['password'] = config.get('HOOK_PASS')
                callback['params']['result'] = {
                    'task': self.name,
                    'status': False,
                    'message': message
                }
                requests.post(callback['hook_url'], json=callback['params'])
        raise Exception(f"no support project from {url}")

    params['project_url'] = tools.normalize_url(url)
    params['domain_name'] = tools.extract_domain(url)
    params['project_key'] = tools.normalize_key(url)
    params['project_hash'] = tools.hash_string(params['project_url'])
    params['raw'] = bool(payload.get('raw'))
    params['opencheck_raw'] = bool(payload.get('opencheck_raw'))
    params['opencheck_raw_param'] = payload.get('opencheck_raw_param') or {}
    params['license'] = bool(payload.get('license'))
    params['identities_load'] = bool(payload.get('identities_load'))
    params['identities_merge'] = bool(payload.get('identities_merge'))
    params['enrich'] = bool(payload.get('enrich'))
    params['raw_enrich_setup'] = payload.get('raw_enrich_setup') or ["git","issue", "issue2", "pull", "pull2", "repo", "stargazer", "fork", "watch", "event"]
    params['panels'] = bool(payload.get('panels'))
    params['level'] = ('community' if level == 'project' or level == 'community' else 'repo')
    params['debug'] = bool(payload.get('debug'))
    params['metrics_activity'] = bool(payload.get('metrics_activity'))
    params['metrics_community'] = bool(payload.get('metrics_community'))
    params['metrics_codequality'] = bool(payload.get('metrics_codequality'))
    params['metrics_group_activity'] = bool(payload.get('metrics_group_activity'))
    params['metrics_domain_persona'] = bool(payload.get('metrics_domain_persona'))
    params['metrics_milestone_persona'] = bool(payload.get('metrics_milestone_persona'))
    params['metrics_role_persona'] = bool(payload.get('metrics_role_persona'))
    params['metrics_criticality_score'] = bool(payload.get('metrics_criticality_score'))
    params['metrics_scorecard'] = bool(payload.get('metrics_scorecard'))
    params['custom_metrics'] = bool(payload.get('custom_metrics'))
    params['metrics_param'] = payload.get('metrics_param')
    params['sleep_for_waiting'] = int(payload.get('sleep_for_waiting') or 5)
    params['force_refresh_enriched'] = bool(payload.get('force_refresh_enriched'))
    params['refresh_sub_repos'] = bool(payload.get('refresh_sub_repos')) if payload.get('refresh_sub_repos') != None else True
    params['from-date'] = payload.get('from-date')
    params['to-date'] = payload.get('to-date')

    return params


@task(name="etl_v1.extract_group", bind=True)
def extract_group(self, *args, **kwargs):
    payload = kwargs['payload']
    url = payload['project_template_yaml']
    level = payload.get('level')
    params = {}
    params['scheme'], params['domain'], params['path'] = tools.extract_url_info(url)
    params['callback'] = callback = payload.get('callback')
    if not (params['domain'] in SUPPORT_DOMAINS):
        message = f"no support project from {url}"
        if validate_callback(callback):
            if callback['params'].get("callback_type", "") != "tpc_software_callback":
                callback['params']['password'] = config.get('HOOK_PASS')
                callback['params']['result'] = {'task': self.name, 'status': False, 'message': message}
                requests.post(callback['hook_url'], json=callback['params'])
        raise Exception(f"no support project from {url}")

    project_yaml_url = tools.normalize_url(url)
    params['project_yaml_url'] = project_yaml_url
    params['project_yaml'] = tools.load_yaml_template(project_yaml_url)
    params['project_key'] = params['project_yaml']['community_name']
    params['project_types'] = params['project_yaml']['resource_types']
    count, gitee_count, github_count, gitcode = tools.count_repos_group(params['project_yaml'])
    data_count = {
        'gitee': gitee_count,
        'github': github_count,
        'gitcode': gitcode,
    }
    params['domain_name'] = max(data_count, key=data_count.get)
    params['project_hash'] = tools.hash_string(params['project_yaml_url'])
    params['raw'] = bool(payload.get('raw'))
    params['identities_load'] = bool(payload.get('identities_load'))
    params['identities_merge'] = bool(payload.get('identities_merge'))
    params['enrich'] = bool(payload.get('enrich'))
    params['raw_enrich_setup'] = payload.get('raw_enrich_setup') or ["git","issue", "issue2", "pull", "pull2", "repo", "stargazer", "fork", "watch", "event"]
    params['panels'] = bool(payload.get('panels'))
    params['level'] = ('community' if level == 'project' or level == 'community' else 'repo')
    params['debug'] = bool(payload.get('debug'))
    params['metrics_activity'] = bool(payload.get('metrics_activity'))
    params['metrics_community'] = bool(payload.get('metrics_community'))
    params['metrics_codequality'] = bool(payload.get('metrics_codequality'))
    params['metrics_group_activity'] = bool(payload.get('metrics_group_activity'))
    params['metrics_domain_persona'] = bool(payload.get('metrics_domain_persona'))
    params['metrics_milestone_persona'] = bool(payload.get('metrics_milestone_persona'))
    params['metrics_role_persona'] = bool(payload.get('metrics_role_persona'))
    params['metrics_criticality_score'] = bool(payload.get('metrics_criticality_score'))
    params['sleep_for_waiting'] = int(payload.get('sleep_for_waiting') or 5)
    params['force_refresh_enriched'] = bool(payload.get('force_refresh_enriched'))
    params['refresh_sub_repos'] = bool(payload.get('refresh_sub_repos')) if payload.get('refresh_sub_repos') != None else True
    params['from-date'] = payload.get('from-date')
    params['to-date'] = payload.get('to-date')

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
    metrics_data = {}
    name = params['project_key']
    domain_name = params['domain_name']
    metrics_data[name] = {}
    for (project_type, project_info) in params['project_types'].items():
        suffix = None
        if tools.is_software_artifact_type(project_type):
            suffix = 'software-artifact'
        if tools.is_governance_type(project_type):
            suffix = 'governance'
        if suffix:
            urls = project_info['repo_urls']
            metrics_data[name][f"{domain_name}-{suffix}"] = list(filter(lambda url: tools.url_is_valid(url), urls))
            for project_url in urls:
                if not tools.url_is_valid(project_url):
                    continue
                url = tools.normalize_url(project_url)
                key = tools.normalize_key(project_url)
                project_data = tools.gen_project_section(project_data, domain_name, key, url)

    project_data_path = join(configs_dir, JSON_NAME)
    with open(project_data_path, 'w') as f:
        json.dump(project_data, f, indent=4, sort_keys=True)

    metrics_data_path = join(metrics_dir, JSON_NAME)
    with open(metrics_data_path, 'w') as jsonfile:
        json.dump(metrics_data, jsonfile, indent=4, sort_keys=True)

    config_logging(params['debug'], logs_dir, False)

    params['metrics_data_path'] = metrics_data_path
    params['project_configs_dir'] = configs_dir
    params['project_logs_dir'] = logs_dir
    params['project_metrics_dir'] = metrics_dir
    params['project_data_path'] = project_data_path

    return params


@task(name="etl_v1.start", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3}, acks_late=True)
def start(*args, **kwargs):
    params = args[0]
    label = params.get('project_url') or params.get('project_key')
    message = {
        'label': label,
        'level': params['level'],
        'origin': params.get('domain_name'),
        'status': 'progress',
        'count': 1 if params['level'] == 'repo' else tools.count_repos(params['project_yaml']),
        'status_updated_at': datetime.isoformat(datetime.utcnow())
    }
    tools.basic_publish('subscriptions_update_v1', message, config.get('RABBITMQ_URI'))
    tools.basic_publish('third_party_callback_v1', message, config.get('RABBITMQ_URI'))
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
    domain_name = params['domain_name']

    input_git_raw_index = f"{domain_name}-git_raw"
    input_git_enriched_index = f"{domain_name}-git_enriched"

    input_repo_raw_index = f"{domain_name}-repo_raw"
    input_repo_enriched_index = f"{domain_name}-repo_enriched"

    input_raw_issues_index = f"{domain_name}-issues_raw"
    input_enrich_issues_index = f"{domain_name}-issues_enriched"

    input_raw_issues2_index = f"{domain_name}2-issues_raw"
    input_enrich_issues2_index = f"{domain_name}2-issues_enriched"

    input_raw_pulls_index = f"{domain_name}-pulls_raw"
    input_enrich_pulls_index = f"{domain_name}-pulls_enriched"

    input_raw_pulls2_index = f"{domain_name}2-pulls_raw"
    input_enrich_pulls2_index = f"{domain_name}2-pulls_enriched"

    input_enrich_releases_index = f"{domain_name}-releases_enriched"

    input_event_raw_index = f"{domain_name}-event_raw"
    input_event_enriched_index = f"{domain_name}-event_enriched"

    input_stargazer_raw_index = f"{domain_name}-stargazer_raw"
    input_stargazer_enriched_index = f"{domain_name}-stargazer_enriched"

    input_fork_raw_index = f"{domain_name}-fork_raw"
    input_fork_enriched_index = f"{domain_name}-fork_enriched"

    #only for gitee
    input_watch_raw_index = f"{domain_name}-watch_raw"
    input_watch_enriched_index = f"{domain_name}-watch_enriched"

    input_refresh_contributors_index = f"{domain_name}-contributors_org_repo"
    input_refresh_contributors_enriched_index = f"{domain_name}-contributors_org_repo_enriched"
    
    # model_index
    metrics_out_index = config.get('METRICS_OUT_INDEX')
    
    model_activity_index = f"{metrics_out_index}_activity"
    model_community_index = f"{metrics_out_index}_community"
    model_codequality_index = f"{metrics_out_index}_codequality"
    model_group_activity_index = f"{metrics_out_index}_group_activity"
    model_domain_persona_index = f"{metrics_out_index}_domain_persona"
    model_milestone_persona_index = f"{metrics_out_index}_milestone_persona"
    model_role_persona_index = f"{metrics_out_index}_role_persona"
    model_criticality_score_index = f"{metrics_out_index}_criticality_score"
    model_scorecard_index = f"{metrics_out_index}_scorecard"
    model_custom_index = f"{metrics_out_index}_custom_v2"
    
    # opencheck index
    input_opencheck_index = "opencheck_raw"
    
    index_version = config.get('INDEX_VERSION')
    if index_version:
        input_git_raw_index = f"{input_git_raw_index}_{index_version}"
        input_git_enriched_index = f"{input_git_enriched_index}_{index_version}"
        input_repo_raw_index = f"{input_repo_raw_index}_{index_version}"
        input_repo_enriched_index = f"{input_repo_enriched_index}_{index_version}"
        input_raw_issues_index = f"{input_raw_issues_index}_{index_version}"
        input_enrich_issues_index = f"{input_enrich_issues_index}_{index_version}"
        input_raw_issues2_index = f"{input_raw_issues2_index}_{index_version}"
        input_enrich_issues2_index = f"{input_enrich_issues2_index}_{index_version}"
        input_raw_pulls_index = f"{input_raw_pulls_index}_{index_version}"
        input_enrich_pulls_index = f"{input_enrich_pulls_index}_{index_version}"
        input_raw_pulls2_index = f"{input_raw_pulls2_index}_{index_version}"
        input_enrich_pulls2_index = f"{input_enrich_pulls2_index}_{index_version}"
        input_enrich_releases_index = f"{input_enrich_releases_index}_{index_version}"
        input_event_raw_index = f"{input_event_raw_index}_{index_version}"
        input_event_enriched_index = f"{input_event_enriched_index}_{index_version}"
        input_stargazer_raw_index = f"{input_stargazer_raw_index}_{index_version}"
        input_stargazer_enriched_index = f"{input_stargazer_enriched_index}_{index_version}"
        input_fork_raw_index = f"{input_fork_raw_index}_{index_version}"
        input_fork_enriched_index = f"{input_fork_enriched_index}_{index_version}"
        input_watch_raw_index = f"{input_watch_raw_index}_{index_version}"
        input_watch_enriched_index = f"{input_watch_enriched_index}_{index_version}"
        input_refresh_contributors_index = f"{input_refresh_contributors_index}_{index_version}"
        input_refresh_contributors_enriched_index = f"{input_refresh_contributors_enriched_index}_{index_version}"
        
        model_activity_index = f"{model_activity_index}_{index_version}"
        model_community_index = f"{model_community_index}_{index_version}"
        model_codequality_index = f"{model_codequality_index}_{index_version}"
        model_group_activity_index = f"{model_group_activity_index}_{index_version}"
        model_domain_persona_index = f"{model_domain_persona_index}_{index_version}"
        model_milestone_persona_index = f"{model_milestone_persona_index}_{index_version}"
        model_role_persona_index = f"{model_role_persona_index}_{index_version}"
        model_criticality_score_index = f"{model_criticality_score_index}_{index_version}"
        model_scorecard_index = f"{model_scorecard_index}_{index_version}"
        model_custom_index = f"{model_custom_index}_{index_version}"
        
        input_opencheck_index = f"{input_opencheck_index}_{index_version}"

    setup['git'] = {
        'raw_index': input_git_raw_index,
        'enriched_index': input_git_enriched_index,
        'category': 'commit',
        'studies': '[enrich_git_branches]' 
    }
    setup['enrich_git_branches'] = {'run_month_days': [i for i in range(1, 32)]}

    issues_cfg = {
        'raw_index': input_raw_issues_index,
        'enriched_index': input_enrich_issues_index,
        'category': 'issue',
        'sleep-for-rate': 'true',
        'no-archive': 'true'
    }

    issues2_cfg = {
        'collect': 'false',
        'raw_index': input_raw_issues_index,
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
        'collect': 'false',
        'raw_index': input_raw_pulls_index,
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

    # only for gitee
    watch_cfg = {
        'raw_index': input_watch_raw_index,
        'enriched_index': input_watch_enriched_index,
        'category': 'watch',
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

    if params.get('from-date'):
        setup['git']['from-date'] = params.get('from-date')
        issues_cfg['from-date'] = params.get('from-date')
        issues2_cfg['from-date'] = params.get('from-date')
        pulls_cfg['from-date'] = params.get('from-date')
        pulls2_cfg['from-date'] = params.get('from-date')
        event_cfg['from-date'] = params.get('from-date')
        fork_cfg['from-date'] = params.get('from-date')
        stargazer_cfg['from-date'] = params.get('from-date')
        watch_cfg['from-date'] = params.get('from-date')
    
    if params.get('to-date'):
        setup['git']['to-date'] = params.get('to-date')
        issues_cfg['to-date'] = params.get('to-date')
        issues2_cfg['to-date'] = params.get('to-date')
        pulls_cfg['to-date'] = params.get('to-date')
        pulls2_cfg['to-date'] = params.get('to-date')
        event_cfg['to-date'] = params.get('to-date')
        fork_cfg['to-date'] = params.get('to-date')
        stargazer_cfg['to-date'] = params.get('to-date')
        watch_cfg['to-date'] = params.get('to-date')
    

    if domain_name in ['gitee', 'gitcode']:
        if domain_name == 'gitee':
            extra = {'api-token': config.get('GITEE_API_TOKEN')}
        if domain_name == 'gitcode':
            extra = {'api-token': config.get('GITCODE_API_TOKEN')}
        if 'issue' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}')
            setup[f'{domain_name}'] = {**issues_cfg, **extra}
        if 'issue2' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}2:issue')
            setup[f'{domain_name}2:issue'] = {**issues2_cfg, **extra}
        if 'pull' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:pull')
            setup[f'{domain_name}:pull'] = {**pulls_cfg, **extra}
        if 'pull2' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}2:pull')
            setup[f'{domain_name}2:pull'] = {**pulls2_cfg, **extra}
        if 'repo' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:repo')
            setup[f'{domain_name}:repo'] = {**repo_cfg, **extra}
        if 'stargazer' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:stargazer')
            setup[f'{domain_name}:stargazer'] = {**stargazer_cfg, **extra}
        if 'fork' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:fork')
            setup[f'{domain_name}:fork'] = {**fork_cfg, **extra}
        if 'event' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:event')
            setup[f'{domain_name}:event'] = {**event_cfg, **extra}
        if 'watch' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:watch')
            setup[f'{domain_name}:watch'] = {**watch_cfg, **extra}
    elif domain_name == 'github':
        extra = {'api-token': config.get('GITHUB_API_TOKEN')}
        graphql_token = {'api-token': config.get('GITHUB_GRAPHQL_API_TOKEN')}
        github_proxy = config.get('GITHUB_PROXY')
        if github_proxy:
            extra['proxy'] = github_proxy
        if 'issue' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:issue')
            setup[f'{domain_name}:issue'] = {**issues_cfg, **extra}
        if 'issue2' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}2:issue')
            setup[f'{domain_name}2:issue'] = {**issues2_cfg, **extra}
        if 'pull' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:pull')
            setup[f'{domain_name}:pull'] = {**pulls_cfg, **extra}
        if 'pull2' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}2:pull')
            setup[f'{domain_name}2:pull'] = {**pulls2_cfg, **extra}
        if 'repo' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}:repo')
            setup[f'{domain_name}:repo'] = {**repo_cfg, **extra}
        if 'event' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}ql:event')
            setup[f'{domain_name}ql:event'] = {**event_cfg, **extra, **graphql_token}
        if 'stargazer' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}ql:stargazer')
            setup[f'{domain_name}ql:stargazer'] = {**stargazer_cfg, **extra, **graphql_token}
        if 'fork' in params['raw_enrich_setup']:
            backends.append(f'{domain_name}ql:fork')
            setup[f'{domain_name}ql:fork'] = {**fork_cfg, **extra, **graphql_token}
    else:
        pass

    project_setup_path = join(params['project_configs_dir'], CFG_NAME)
    with open(project_setup_path, 'w') as cfg:
        setup.write(cfg)

    params['project_setup_path'] = project_setup_path
    params['project_backends'] = backends
    params['project_issues_index'] = input_enrich_issues_index
    params['project_issues2_index'] = input_enrich_issues2_index
    params['project_pulls_index'] = input_enrich_pulls_index
    params['project_pulls2_index'] = input_enrich_pulls2_index
    params['project_git_index'] = input_git_enriched_index
    params['project_repo_index'] = input_repo_enriched_index
    params['project_release_index'] = input_enrich_releases_index

    params['project_event_index'] = input_event_enriched_index
    params['project_fork_index'] = input_fork_enriched_index
    params['project_stargazer_index'] = input_stargazer_enriched_index

    # no use
    params['project_watch_index'] = input_watch_enriched_index

    params['project_contributors_index'] = input_refresh_contributors_index
    params['project_contributors_enriched_index'] = input_refresh_contributors_enriched_index
    
    # model index
    params['model_activity_index'] = model_activity_index
    params['model_community_index'] = model_community_index
    params['model_codequality_index'] = model_codequality_index
    params['model_group_activity_index'] = model_group_activity_index
    params['model_domain_persona_index'] = model_domain_persona_index
    params['model_milestone_persona_index'] = model_milestone_persona_index
    params['model_role_persona_index'] = model_role_persona_index
    params['model_criticality_score_index'] = model_criticality_score_index
    params['model_scorecard_index'] = model_scorecard_index
    params['model_custom_index'] = model_custom_index
    
    params['project_opencheck_index'] = input_opencheck_index
 
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


@task(name="etl_v1.expire_enriched", autoretry_for=(Exception,), acks_late=True)
def expire_enriched(*args, **kwargs):
    params = args[0]
    config_logging(params['debug'], params['project_logs_dir'])
    params['force_refresh_enriched_started_at'] = datetime.now()
    if params.get('force_refresh_enriched'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)
        repo_urls = []
        if params.get('level') == 'repo':
            repo_urls = [params['project_url']]
        else:
            for (project_type, project_info) in params['project_types'].items():
                suffix = None
                if tools.is_software_artifact_type(project_type):
                    suffix = 'software-artifact'
                if tools.is_governance_type(project_type):
                    suffix = 'governance'
                if suffix:
                    urls = list(filter(lambda url: tools.url_is_valid(url), project_info['repo_urls']))
                    repo_urls.extend(urls)

        for repo_url in repo_urls:
            for index in [
                    'project_git_index',
                    'project_issues_index',
                    'project_issues2_index',
                    'project_pulls_index',
                    'project_pulls2_index'
            ]:
                body = {
                    "query": {
                        "match": {
                            "tag": f"{repo_url}.git" if 'git' in index else repo_url
                        }
                    }
                }
                es_client.delete_by_query(index=params[index], body=body)
        params['force_refresh_enriched_finished_at'] = datetime.now()
    else:
        params['force_refresh_enriched_finished_at'] = 'skipped'

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


@task(name="etl_v1.identities", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
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

@task(name="etl_v1.sleep", acks_late=True)
def sleep(*args, **kwargs):
    params = args[0]
    if params.get('sleep_for_waiting'):
        time.sleep(params['sleep_for_waiting'])
    return params


@task(name="etl_v1.contributors_refresh", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def contributors_refresh(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['contributors_refresh_started_at'] = datetime.now()
    if params['identities_load'] or params['identities_merge']:
        from_date = params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE')
        from_date = (datetime.strptime(from_date, "%Y-%m-%d") - relativedelta(months=4)).strftime("%Y-%m-%d")

        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'json_file': params['metrics_data_path'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'git_index': params['project_git_index'],
            'contributors_index': params['project_contributors_index'],
            'contributors_enriched_index': params['project_contributors_enriched_index'],
            'from_date': from_date,
            'end_date': params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d'),
            'repo_index': params['project_repo_index'],
            'event_index': params['project_event_index'],
            'stargazer_index': params['project_stargazer_index'],
            'fork_index': params['project_fork_index'],
            'level': params['level'],
            'community': project_key,
            'contributors_org_index': 'contributor_org',
            'organizations_index': 'organizations',
            'bots_index': 'bots',
            'company': None
        }
        params["contributors_refresh_params"] = metrics_cfg
        contributor_refresh = ContributorDevOrgRepo(**metrics_cfg['params'])
        contributor_refresh.run(metrics_cfg['url'])
        params['contributors_refresh_finished_at'] = datetime.now()
    else:
        params['contributors_refresh_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.metrics.activity", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
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
            'out_index': params['model_activity_index'],
            'git_branch': None,
            'from_date': params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE'),
            'end_date': params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'release_index': params['project_release_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index']
        }
        params['metrics_activity_params'] = metrics_cfg
        model_activity = ActivityMetricsModel(**metrics_cfg['params'])
        model_activity.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_activity_finished_at'] = datetime.now()
    else:
        params['metrics_activity_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.metrics.community", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
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
            'out_index': params['model_community_index'],
            'from_date': params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE'),
            'end_date': params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d'),
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

    if params.get('metrics_codequality'):
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'repo_index': params['project_repo_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'out_index': params['model_codequality_index'],
            'git_branch': None,
            'from_date': params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE'),
            'end_date': params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'company': None,
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index']
        }
        params['metrics_codequality_params'] = metrics_cfg
        model_codequality = CodeQualityGuaranteeMetricsModel(**metrics_cfg['params'])
        model_codequality.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_codequality_finished_at'] = datetime.now()
    else:
        params['metrics_codequality_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.metrics.group_activity", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
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
            'out_index': params['model_group_activity_index'],
            'git_branch': None,
            'from_date': params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE'),
            'end_date': params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'company': None,
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index']
        }
        params['metrics_group_activity_params'] = metrics_cfg
        model_codequality = OrganizationsActivityMetricsModel(**metrics_cfg['params'])
        model_codequality.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_group_activity_finished_at'] = datetime.now()
    else:
        params['metrics_group_activity_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.metrics.domain_persona", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_domain_persona(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_domain_persona_started_at'] = datetime.now()

    if params.get('metrics_domain_persona'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)
        out_index = params['model_domain_persona_index']
        from_date = params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE')
        end_date = params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d')
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': params['project_repo_index'],
            'git_index': params['project_git_index'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index'],
            'out_index': out_index,
            'from_date': from_date,
            'end_date': end_date,
            'level': params['level'],
            'community': project_key,
            'source': params['domain_name'],
            'json_file': params['metrics_data_path'],
            'contributors_enriched_index': params['project_contributors_enriched_index']
        }
        params['metrics_domain_persona_params'] = metrics_cfg
        model_domain_persona = DomainPersonaMetricsModel(**metrics_cfg['params'])
        model_domain_persona.metrics_model_metrics(metrics_cfg['url'])
        if params['level'] == 'community' and params.get('refresh_sub_repos'):
            tools.check_sub_repos_metrics(es_client, out_index, params['project_types'],
                                          {'metrics_domain_persona': True, 'from-date': from_date, 'to-date': end_date})
        params['metrics_domain_persona_finished_at'] = datetime.now()
    else:
        params['metrics_domain_persona_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.metrics.milestone_persona", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_milestone_persona(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_milestone_persona_started_at'] = datetime.now()

    if params.get('metrics_milestone_persona'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)
        out_index = params['model_milestone_persona_index']
        from_date = params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE')
        end_date = params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d')
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': params['project_repo_index'],
            'git_index': params['project_git_index'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index'],
            'out_index': out_index,
            'from_date': params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE'),
            'end_date': end_date,
            'level': params['level'],
            'community': project_key,
            'source': params['domain_name'],
            'json_file': params['metrics_data_path'],
            'contributors_enriched_index': params['project_contributors_enriched_index']
        }
        params["metrics_milestone_persona_params"] = metrics_cfg
        model_milestone_persona = MilestonePersonaMetricsModel(**metrics_cfg['params'])
        model_milestone_persona.metrics_model_metrics(metrics_cfg['url'])
        if params['level'] == 'community' and params.get('refresh_sub_repos'):
            tools.check_sub_repos_metrics(es_client, out_index, params['project_types'],
                                          {'metrics_milestone_persona': True, 'from-date': from_date, 'to-date': end_date})
        params['metrics_milestone_persona_finished_at'] = datetime.now()
    else:
        params['metrics_milestone_persona_finished_at'] = 'skipped'
    return params

@task(name="etl_v1.metrics.role_persona", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_role_persona(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_role_persona_started_at'] = datetime.now()

    if params.get('metrics_role_persona'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)
        out_index = params['model_role_persona_index']
        from_date = params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE')
        end_date = params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d')
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': params['project_repo_index'],
            'git_index': params['project_git_index'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index'],
            'out_index': out_index,
            'from_date': params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE'),
            'end_date': end_date,
            'level': params['level'],
            'community': project_key,
            'source': params['domain_name'],
            'json_file': params['metrics_data_path'],
            'contributors_enriched_index': params['project_contributors_enriched_index']
        }
        params['metrics_role_persona_params'] = metrics_cfg
        model_role_persona = RolePersonaMetricsModel(**metrics_cfg['params'])
        model_role_persona.metrics_model_metrics(metrics_cfg['url'])
        if params['level'] == 'community' and params.get('refresh_sub_repos'):
            tools.check_sub_repos_metrics(es_client, out_index, params['project_types'],
                                          {'metrics_role_persona': True, 'from-date': from_date, 'to-date': end_date})
        params['metrics_role_persona_finished_at'] = datetime.now()
    else:
        params['metrics_role_persona_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.finish", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3}, acks_late=True)
def finish(*args, **kwargs):
    params = args[0][0] if type(args[0]) == list else args[0]
    label = params.get('project_url') or params.get('project_key')
    message = {
        'label': label,
        'level': params['level'],
        'origin': params.get('domain_name'),
        'status': 'complete',
        'count': 1 if params['level'] == 'repo' else tools.count_repos(params['project_yaml']),
        'status_updated_at': datetime.isoformat(datetime.utcnow())
    }
    tools.basic_publish('subscriptions_update_v1', message, config.get('RABBITMQ_URI'))
    tools.basic_publish('third_party_callback_v1', message, config.get('RABBITMQ_URI'))
    return params

@task(name="etl_v1.notify", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def notify(*args, **kwargs):
    params = args[0][0] if type(args[0]) == list else args[0]
    callback = params['callback']
    target = params.get('project_url') or params.get('project_key')
    level = params.get('level') or 'repo'
    domain_name = params.get('domain_name')
    if validate_callback(callback):
        if callback['params'].get("callback_type", "") != "tpc_software_callback":
            label = urllib.parse.quote(target, safe='')
            compass_host = "https://oss-compass.org"
            if domain_name in ['gitee', 'gitcode']:
                compass_host = "https://compass.gitee.com"
            report_url = f"{compass_host}/analyze?label={label}&level={level}"
            callback['params']['password'] = config.get('HOOK_PASS')
            callback['params']['domain'] = params['domain_name']
            callback['params']['result'] = {'status': True, 'message': f"The analysis you submitted has been completed, and the address of the analysis report is: Report Link: {report_url}"}
            resp = requests.post(callback['hook_url'], json=callback['params'])
            return {'status': True, 'code': resp.status_code, 'message': resp.text}
        else:
            callback['params']['project_url'] = target + ".git"
            callback['params']['command_list'] = ["compass"]
            callback['params']['scan_results'] = { "compass": { "status": True } }
            resp = requests.post(callback['hook_url'], json=callback['params'])
            return {'status': True, 'code': resp.status_code, 'message': resp.text}
    else:
        return {'status': False, 'message': 'no callback'}

@task(name="etl_v1.license", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def license(*args, **kwargs):
    params = args[0][0] if type(args[0]) == list else args[0]
    label = params.get('project_url') or params.get('project_key')
    if not params['license']:
        params['license_finished_at'] = 'skipped'
        return params

    payload = {
        "username": config.get('TPC_SERVICE_API_USERNAME'),
        "password": config.get('TPC_SERVICE_API_PASSWORD')
    }

    def base_post_request(request_path, payload, token=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            TPC_SERVICE_API_ENDPOINT = config.get('TPC_SERVICE_API_ENDPOINT')
            response = requests.post(
                f"{TPC_SERVICE_API_ENDPOINT}/{request_path}",
                json=payload,
                headers=headers
            )
            response.raise_for_status()
            resp_data = response.json()
            if "error" in resp_data:
                return {"status": False, "message": f"Error: {resp_data.get('description', 'Unknown error')}"}
            return {"status": True, "body": resp_data}
        except requests.RequestException as ex:
            return {"status": False, "message": str(ex)}

    result = base_post_request("auth", payload)
    if not result["status"]:
        return {'status': False, 'message': 'no auth'}
    token = result["body"]["access_token"]

    commands = ["scancode","osv-scanner"]
    TPC_SERVICE_CALLBACK_URL = config.get("TPC_SERVICE_SERVICE_CALLBACK_URL")
    payload = {
        "commands": commands,
        "project_url": f"{label}.git",
        "callback_url": TPC_SERVICE_CALLBACK_URL,
        "task_metadata": {
            "report_type": -1
        }
    }
    result = base_post_request("opencheck", payload, token=token)
    # print(f"Analyze metric by TPC service info: {result}")
    if result["status"]:
        license_result = {'status': True, 'message': result['body']}
        params["license_result"] = license_result
        return params
    else:
        license_result = {'status': False, 'message': 'no callback'}
        params["license_result"] = license_result
        return params


@task(name="etl_v1.metrics.custom_metrics", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metric(*args, **kwargs):
    params = args[0][0] if type(args[0]) == list else (args)[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])

    params['custom_metrics_started_at'] = datetime.now()
    if params.get('custom_metrics'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)


        metrics_param = params.get('metrics_param')
        # custom metrics_param
        # {
        #     "metric_list": ["commit_frequency", "bug_issue_open_time"],
        #     "version_number": "v1.23"
        # }

        out_index = params['model_custom_index']
        from_date = params.get('from-date') if params.get('from-date') else config.get('METRICS_FROM_DATE')
        end_date = params.get('to-date') if params.get('to-date') else datetime.now().strftime('%Y-%m-%d')
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': params['project_repo_index'],
            'git_index': params['project_git_index'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index'],
            'out_index': out_index,
            'from_date': from_date,
            'end_date': end_date,
            'level': params['level'],
            'community': project_key,
            'source': params['domain_name'],
            'json_file': params['metrics_data_path'],
            'contributors_enriched_index': params['project_contributors_enriched_index'],
            'metrics_param': metrics_param
        }
        params['custom_metrics_params'] = metrics_cfg

        model_role_persona = MetricsModelCustom(**metrics_cfg['params'])
        model_role_persona.metrics_model_custom(metrics_cfg['url'])

        params['custom_metrics'] = datetime.now()
    else:
        params['custom_metrics_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.metrics.criticality_score", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_criticality_score(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_criticality_score_started_at'] = datetime.now()

    if params.get('metrics_criticality_score'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)
        out_index = params['model_criticality_score_index']
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': params['project_repo_index'],
            'git_index': params['project_git_index'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index'],
            'out_index': out_index,
            'from_date': from_date,
            'end_date': end_date,
            'level': params['level'],
            'community': project_key,
            'source': params['domain_name'],
            'json_file': params['metrics_data_path']
        }
        params['metrics_criticality_score_params'] = metrics_cfg
        model_criticality_score = CriticalityScoreMetricsModel(**metrics_cfg['params'])
        model_criticality_score.metrics_model_metrics(metrics_cfg['url'])
        if params['level'] == 'community' and params.get('refresh_sub_repos'):
            tools.check_sub_repos_metrics(es_client, out_index, params['project_types'],
                                          {'metrics_criticality_score': True, 'from-date': from_date, 'to-date': end_date})
        params['metrics_criticality_score_finished_at'] = datetime.now()
    else:
        params['metrics_criticality_score_finished_at'] = 'skipped'
    return params


@task(name="etl_v1.opencheck_raw", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def opencheck_raw(*args, **kwargs):
    params = args[0][0] if type(args[0]) == list else args[0]
    label = params.get('project_url') or params.get('project_key')
    if not params['opencheck_raw']:
        params['opencheck_raw_finished_at'] = 'skipped'
        return params

    payload = {
        "username": config.get('TPC_SERVICE_API_USERNAME'),
        "password": config.get('TPC_SERVICE_API_PASSWORD')
    }

    def base_post_request(request_path, payload, token=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            TPC_SERVICE_API_ENDPOINT = config.get('TPC_SERVICE_API_ENDPOINT')
            response = requests.post(
                f"{TPC_SERVICE_API_ENDPOINT}/{request_path}",
                json=payload,
                headers=headers
            )
            response.raise_for_status()
            resp_data = response.json()
            if "error" in resp_data:
                return {"status": False, "message": f"Error: {resp_data.get('description', 'Unknown error')}"}
            return {"status": True, "body": resp_data}
        except requests.RequestException as ex:
            return {"status": False, "message": str(ex)}

    result = base_post_request("auth", payload)
    if not result["status"]:
        return {'status': False, 'message': 'no auth'}
    token = result["body"]["access_token"]

    commands = params['opencheck_raw_param'].get('commands')
    if not commands:
        default_commands = config.get('TPC_SERVICE_OPENCHECK_COMMANDS')
        commands = json.loads(default_commands)
        commands = list(set(commands))
    access_token = params['opencheck_raw_param'].get('access_token')
    
    TPC_SERVICE_CALLBACK_URL = config.get("TPC_SERVICE_SERVICE_CALLBACK_URL")
    metrics_model_list = []
    metrics = ['scorecard', 'criticality_score']
    for metric in metrics:
        if params.get(f"metrics_{metric}"):
            metrics_model_list.append(metric)
    payload = {
        "commands": commands,
        "project_url": label,
        "callback_url": TPC_SERVICE_CALLBACK_URL,
        "access_token": access_token,
        "task_metadata": {
            "report_type": -2,
            "metrics_model": metrics_model_list
        }
    }
    result = base_post_request("opencheck", payload, token=token)
    if result["status"]:
        opencheck_raw_result = {'status': True, 'message': result['body']}
        params["opencheck_raw_result"] = opencheck_raw_result
        return params
    else:
        opencheck_raw_result = {'status': False, 'message': 'no callback'}
        params["opencheck_raw_result"] = opencheck_raw_result
        return params
    

@task(name="etl_v1.metrics.scorecard", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_scorecard(*args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_scorecard_started_at'] = datetime.now()

    if params.get('metrics_scorecard'):
        elastic_url = config.get('ES_URL')
        is_https = urlparse(elastic_url).scheme == 'https'
        es_client = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection,
            timeout=180, max_retries=3, retry_on_timeout=True)
        out_index = params['model_scorecard_index']
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'repo_index': params['project_repo_index'],
            'git_index': params['project_git_index'],
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'issue_comments_index': params['project_issues2_index'],
            'pr_comments_index': params['project_pulls2_index'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index'],
            'out_index': out_index,
            'from_date': from_date,
            'end_date': end_date,
            'level': params['level'],
            'community': project_key,
            'source': params['domain_name'],
            'json_file': params['metrics_data_path'],
            'contributors_enriched_index': params['project_contributors_enriched_index'],
            'openchecker_index': params['project_opencheck_index'],
        }
        params['metrics_scorecard_params'] = metrics_cfg
        
        model_scorecard = ScorecardMetricsModel(**metrics_cfg['params'])
        model_scorecard.metrics_model_metrics(metrics_cfg['url'])
        
        params['metrics_scorecard_finished_at'] = datetime.now()
    else:
        params['metrics_scorecard_finished_at'] = 'skipped'
    return params