import requests
import tldextract
import yaml
import hashlib
import re

from director import task, config
from urllib.parse import urlparse

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
