import requests
import tldextract
import yaml
import time
import hashlib
import re

from director import task, config
from urllib.parse import urlparse
from dateutil import parser
from datetime import datetime, timedelta

import pika
import json
import traceback

import logging

logger = logging.getLogger(__name__)

def basic_publish(queue, message, url_params):
    params = pika.URLParameters(url_params)
    params.socket_timeout = 15
    connection = None
    try:
        connection = pika.BlockingConnection(params)  # Connect to CloudAMQP
        channel = connection.channel()
        channel.basic_publish(exchange='', routing_key=queue,
                              body=json.dumps(message))
    except Exception as e:
        output = traceback.format_exc()
        logger.warning(f"Exception while sending a message to the bot: {output}")
        raise e
    finally:
        if connection:
            connection.close()

def extract_url_info(url):
    uri = urlparse(url)
    return uri.scheme, uri.netloc, uri.path

def extract_domain(url):
    uri = urlparse(url)
    return 'gitee' if tldextract.extract(uri.netloc).domain == 'gitee' else 'github'

def extract_path(url):
    uri = urlparse(url)
    return uri.path

def normalize_url(url):
    uri = urlparse(url)
    return f"{uri.scheme}://{uri.netloc}{uri.path}"

def normalize_key(url):
    uri = urlparse(url)
    domain_name = tldextract.extract(uri.netloc).domain
    return f"{domain_name}{uri.path.replace('/', '-')}".lower()

def hash_string(string):
    h = hashlib.new('sha256')
    h.update(bytes(string, encoding='utf-8'))
    return h.hexdigest()

def is_software_artifact_type(project_type):
    return project_type == 'software-artifact-repositories' or \
        project_type == 'software-artifact-resources' or \
        project_type == 'software-artifact-projects'

def is_governance_type(project_type):
    return project_type == 'governance-repositories' or \
        project_type == 'governance-resources' or \
        project_type == 'governance-projects'

def check_sub_repos_metrics(es_client, out_index, project_types, metrics_payload):
    repo_urls = []
    for (project_type, project_info) in project_types.items():
        suffix = None
        if is_software_artifact_type(project_type):
            suffix = 'software-artifact'
        if is_governance_type(project_type):
            suffix = 'governance'
        if suffix:
            urls = list(filter(lambda url: url_is_valid(url), project_info['repo_urls']))
            repo_urls.extend(urls)
    for repo_url in set(repo_urls):
        last_time = get_last_metrics_model_time(es_client, out_index, repo_url, 'repo')
        if last_time is None or (last_time is not None and parser.parse(last_time).replace(tzinfo=None) < (datetime.now() - timedelta(days=7))):
            logger.warning(f"Begin to refresh {repo_url} due to expired already {last_time}.")
            run_single_repo_workflow(repo_url, extra_payload=metrics_payload)

def run_single_repo_workflow(repo_url, extra_payload={}):
    json_data = {
        'project': 'insight',
        'name': 'ETL_V1',
        'payload': {
            'debug': False,
            'enrich': False,
            'identities_load': False,
            'identities_merge': False,
            'force_refresh_enriched': False,
            'metrics_activity': False,
            'metrics_codequality': False,
            'metrics_community': False,
            'metrics_group_activity': False,
            'panels': False,
            'project_url': repo_url,
            'raw': False,
        }
    }
    json_data['payload'].update(extra_payload)
    max_retries = 5
    retry_interval = 15  # seconds
    for attempt in range(max_retries):
        response = requests.post(f"{config.get('DEFAULT_HOST')}/api/workflows", json=json_data, verify=False)
        if response.status_code >= 200 and response.status_code < 300:
            break
        else:
            logger.warning(f"Request failed with status code {response.status_code}. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)

    return response

def get_last_metrics_model_time(es_client, index, label, level):
    try:
        query_hits = es_client.search(index=index, body=get_last_metrics_model_query(label, level))["hits"]["hits"]
        return query_hits[0]["_source"]["grimoire_creation_date"] if query_hits.__len__() > 0 else None
    except NotFoundError:
        return None

def get_last_metrics_model_query(label, level):
    query = {
        'size': 1,
        'query': {
            'bool': {
                'must': [
                    {
                        'match_phrase': {
                            'label': label
                        }
                    },
                    {
                        'term': {
                            'level.keyword': level
                        }
                    }
                ]
            }
        },
        'sort': [
            {
                'grimoire_creation_date': {
                    'order': 'desc',
                    'unmapped_type': 'keyword'
                }
            }
        ]
    }
    return query

def url_is_valid(url):
    regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None

def gen_project_section(project_data, domain_name, key, url):
    if domain_name == 'gitee':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][domain_name] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
        project_data[key][f"{domain_name}:event"] = [url]
        project_data[key][f"{domain_name}:stargazer"] = [url]
        project_data[key][f"{domain_name}:fork"] = [url]
        project_data[key][f"{domain_name}:watch"] = [url]
    elif domain_name == 'github':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][f"{domain_name}:issue"] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
        project_data[key][f"{domain_name}ql:event"] = [url]
        project_data[key][f"{domain_name}ql:stargazer"] = [url]
        project_data[key][f"{domain_name}ql:fork"] = [url]
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



def count_repos_group(yaml):
    count, gitee_count, github_count = 0, 0, 0
    for (project_type, project_info) in yaml['resource_types'].items():
        suffix = None
        if project_type == 'software-artifact-repositories' or \
           project_type == 'software-artifact-resources' or \
           project_type == 'software-artifact-projects':
            suffix = 'software-artifact'
        if project_type == 'governance-repositories' or \
           project_type == 'governance-resources' or \
           project_type == 'governance-projects':
            suffix = 'governance'
        if suffix:
            urls = project_info['repo_urls']
            for project_url in urls:
                if not url_is_valid(project_url):
                    continue
                if extract_domain(project_url) == 'gitee':
                    gitee_count += 1
                if extract_domain(project_url) == 'github':
                    github_count += 1
                count += 1
    return count, gitee_count, github_count

def count_repos(yaml):
    return count_repos_group(yaml)[0]
