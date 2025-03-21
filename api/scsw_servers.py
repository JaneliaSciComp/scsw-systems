''' scsw_servers.py

    Flask application to ping/display the SCSW servers from
    https://wikis.janelia.org/display/ScientificComputing/SCSW+Servers
'''

__version__ = "2.1.0"

from collections import namedtuple
from datetime import datetime, timedelta, timezone
import inspect
from json import JSONEncoder, loads
import os
import platform
import re
from socket import socket
import subprocess
import sys
from time import time
from atlassian import Confluence
from bs4 import BeautifulSoup
import dask.distributed
from distributed.utils import TimeoutError as DaskTimeoutError
from flask import (Flask, make_response, render_template, request, jsonify)
from flask_cors import CORS
from flask_swagger import swagger
import idna
from OpenSSL import SSL
import requests

# pylint: disable=broad-exception-caught,broad-exception-raised

# Navigation
NAV = {"Home": "",
       "VMs": {"Nutanix VMs": "nutanix"},
      }
# Dates
OPSTART = datetime.strptime('2024-05-16','%Y-%m-%d')
# General
WIKI_URL = "https://wikis.janelia.org"
SCICOMP = f"{WIKI_URL}/display/ScientificComputing"
HostInfo = namedtuple(field_names='cert hostname peername', typename='HostInfo')

# ******************************************************************************
# * Classes                                                                    *
# ******************************************************************************

class CustomJSONEncoder(JSONEncoder):
    ''' Define a custom JSON encoder
    '''
    def default(self, o):
        try:
            if isinstance(o, datetime):
                return o.strftime("%a, %-d %b %Y %H:%M:%S")
            if isinstance(o, timedelta):
                seconds = o.total_seconds()
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                seconds = seconds % 60
                return f"{hours:02d}:{minutes:02d}:{seconds:.02f}"
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, o)


class InvalidUsage(Exception):
    ''' Class to populate error return for JSON.
    '''
    def __init__(self, message, status_code=400, payload=None):
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code
        self.payload = payload
    def to_dict(self):
        ''' Build error response
        '''
        retval = dict(self.payload or ())
        retval['rest'] = {'status_code': self.status_code,
                          'error': True,
                          'error_text': f"{self.message}\n" \
                                        + f"An exception of type {type(self).__name__} occurred. " \
                                        + f"Arguments:\n{self.args}"}
        return retval


class CustomException(Exception):
    ''' Class to populate error return for HTML.
    '''
    def __init__(self,message, preface=""):
        super().__init__(message)
        self.original = type(message).__name__
        self.args = message.args
        cfunc = inspect.stack()[1][3]
        self.preface = f"In {cfunc}, {preface}" if preface else f"Error in {cfunc}."

# ******************************************************************************
# * Flask                                                                      *
# ******************************************************************************

app = Flask(__name__, template_folder="templates")
app.json_encoder = CustomJSONEncoder
app.config.from_pyfile("config.cfg")
CORS(app, supports_credentials=True)
app.config["STARTDT"] = datetime.now()
app.config["LAST_TRANSACTION"] = time()

@app.before_request
def before_request():
    ''' Set transaction start time and increment counters.
        If needed, initilize global variables.
    '''
    app.config["START_TIME"] = time()
    app.config["COUNTER"] += 1
    endpoint = request.endpoint if request.endpoint else "(Unknown)"
    app.config["ENDPOINTS"][endpoint] = app.config["ENDPOINTS"].get(endpoint, 0) + 1
    if request.method == "OPTIONS":
        result = initialize_result()
        return generate_response(result)
    return None

# ******************************************************************************
# * Error utility functions                                                    *
# ******************************************************************************

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    ''' Error handler
        Keyword arguments:
          error: error object
    '''
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def error_message(err):
    ''' Create an error message from an exception
        Keyword arguments:
          err: exception
        Returns:
          Error message
    '''
    if isinstance(err, CustomException):
        msg = f"{err.preface}\n" if err.preface else ""
        msg += f"An exception of type {err.original} occurred. Arguments:\n{err.args}"
    else:
        msg = f"An exception of type {type(err).__name__} occurred. Arguments:\n{err.args}"
    return msg


def inspect_error(err, errtype):
    ''' Render an error with inspection
        Keyword arguments:
          err: exception
        Returns:
          Error screen
    '''
    mess = f"In {inspect.stack()[1][3]}, An exception of type {type(err).__name__} occurred. " \
           + f"Arguments:\n{err.args}"
    return render_template('error.html', urlroot=request.url_root,
                           title=render_warning(errtype), message=mess)


def render_warning(msg, severity='error', size='lg'):
    ''' Render warning HTML
        Keyword arguments:
          msg: message
          severity: severity (warning, error, info, or success)
          size: glyph size
        Returns:
          HTML rendered warning
    '''
    icon = 'exclamation-triangle'
    color = 'goldenrod'
    if severity == 'error':
        color = 'red'
    elif severity == 'success':
        icon = 'check-circle'
        color = 'lime'
    elif severity == 'info':
        icon = 'circle-info'
        color = 'blue'
    elif severity == 'na':
        icon = 'minus-circle'
        color = 'gray'
    elif severity == 'missing':
        icon = 'minus-circle'
    elif severity == 'no':
        icon = 'times-circle'
        color = 'red'
    elif severity == 'warning':
        icon = 'exclamation-circle'
    return f"<span class='fas fa-{icon} fa-{size}' style='color:{color}'></span>" \
           + f"&nbsp;{msg}"

# *****************************************************************************
# * Processing functions                                                      *
# *****************************************************************************

def find_servers():
    ''' Find servers from the wiki page
        Keyword arguments:
          None
        Returns:
          server: dictionary of server details
    '''
    token = os.environ.get('CONFLUENCE_API_TOKEN') \
        if os.environ.get('CONFLUENCE_API_TOKEN') else None
    if not token:
        raise Exception("Missing token - set in CONFLUENCE_API_TOKEN environment variable")
    confluence = Confluence(url=WIKI_URL, token=token)
    content = confluence.get_page_by_title(space='ScientificComputing', title='SCSW Servers')
    if not content or "id" not in content:
        raise Exception("Failed to retrieve SCSW Servers page")
    content = confluence.get_page_by_id(page_id=content["id"], expand="body.view")
    soup = BeautifulSoup(content['body']['view']['value'], 'html.parser')
    tables = soup.find_all('table')
    server = {}
    divisions = ['Physical', 'Virtual']
    cnt = 0
    for i, table in enumerate(tables, 1):
        if i > 2:
            break
        stype = divisions.pop(0)
        for row in table.find_all('tr')[1:]:
            row_data = [td.get_text(strip=True) for td in row.find_all('td')]
            if any(row_data):
                if 'Template' in row_data[0]:
                    continue
                short = re.sub(r".*Server - ", "", row_data[0])
                if short in server:
                    raise Exception(f"Duplicate server: {row_data[0]}")
                cnt += 1
                if not row_data[4] or 'retired' in row_data[1].lower() \
                    or 'retired' in row_data[6].lower():
                    server[short] = {"title": short,
                                      "description": row_data[1],
                                      "ip": row_data[4],
                                      "canonical": row_data[5],
                                      "aliases": row_data[6],
                                      "suffix": row_data[0].replace(" ", "+"),
                                      "type": 'Retired'}
                else:
                    server[short] = {"title": short,
                                     "description": row_data[1],
                                     "ip": row_data[4],
                                     "canonical": row_data[5],
                                     "aliases": row_data[6],
                                     "suffix": row_data[0].replace(" ", "+"),
                                     "type": stype,
                                     "cert": row_data[5] if row_data[7] else None}
    print(f"Found {cnt} servers")
    return server


def get_certificate(hostname, port=443):
    """
    Retrieve the certificate from the given host and port.
    Args:
        host (str): The hostname to connect to.
        port (int): The port to connect to.
    Returns:
        HostInfo: An object containing the certificate and other host information.
    """
    hostname_idna = idna.encode(hostname)
    sock = socket()
    try:
        sock.connect((hostname, port))
    except Exception as err:
        print(err)
        return HostInfo(cert=None, peername=None, hostname=hostname)
    peername = sock.getpeername()
    ctx = SSL.Context(SSL.SSLv23_METHOD) # most compatible
    ctx.check_hostname = False
    ctx.verify_mode = SSL.VERIFY_NONE
    sock_ssl = SSL.Connection(ctx, sock)
    sock_ssl.set_connect_state()
    sock_ssl.set_tlsext_host_name(hostname_idna)
    sock_ssl.do_handshake()
    cert = sock_ssl.get_peer_certificate()
    crypto_cert = cert.to_cryptography()
    sock_ssl.close()
    sock.close()
    return HostInfo(cert=crypto_cert, peername=peername, hostname=hostname)


def get_nutanix():
    ''' Get Nutanix VMs
    '''
    base_url = "https://jrc-ntnx.int.janelia.org:9440/PrismGateway/services/rest/v1/"
    service = "vms/"
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    session = requests.Session()
    session.auth = (app.config['NUTANIX_USER'], app.config['NUTANIX_PASS'])
    session.headers.update(headers)
    session.verify = True
    url = base_url + service
    response = session.get(url)
    if response.status_code == 200:
        data = loads(response.text)
        vms = {}
        for vm in data['entities']:
            if not vm['vmName'].startswith('vm'):
                continue
            if ' - ' in vm['vmName']:
                name, alias = vm['vmName'].split(' - ', 1)
            else:
                name, alias = vm['vmName'].split(' ', 1)
            vms[name] = {"alias": alias,
                         "cpus": vm['numVCpus'],
                         "memory": vm['memoryCapacityInBytes'],
                         "disk": vm['diskCapacityInBytes'],
                         "power": vm['powerState'],
                         "ip": vm['ipAddresses'][0] if vm['ipAddresses'] else 'N/A'}
    else:
        raise Exception(f"Failed to retrieve Nutanix VMs: {response.status_code}")
    return vms


def humansize(num, suffix='B', places=2, space='disk'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
          space: "disk" or "mem"
        Returns:
          string
    '''
    limit = 1024.0 if space == 'disk' else 1000.0
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < limit:
            return f"{num:.{places}f}{unit}{suffix}"
        num /= limit
    return "{num:.1f}P{suffix}"


def ping(host):
    ''' Ping a host
        Keyword arguments:
          host: host IP to ping
        Returns:
          True if host responds to a ping request, False otherwise
    '''
    host = host.replace(' ', '')
    param = '-n' if platform.system().lower()=='windows' else '-c'
    command = ['ping', param, '1', host]
    try:
        output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=2, check=False)
        return output.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception as err:
        print(f"Error pinging {host}: {err}")
        return False


def http_reachable(host):
    ''' Check if a host is reachable via HTTP
        Keyword arguments:
          host: host to check
        Returns:
          True if host is reachable via HTTP, False otherwise
    '''
    host = host.replace(' ', '')
    try:
        response = requests.get(f"https://{host}", timeout=2)
        print(f"HTTP response for {host}: {response.status_code}")
        return response.status_code == 200
    except Exception:
        return False


def multiping(details):
    ''' Ping a hosts associated with one server name
        Keyword arguments:
          details: dictionary of server details
        Returns:
          False if host responds to all ping requests, otherwise list of bad hosts
    '''
    bad_hosts = []
    if details['canonical'] and not ping(details['canonical']) \
       and not http_reachable(details['canonical']):
        bad_hosts.append(details['canonical'])
    if details['ip'] and not ping(details['ip']) and not http_reachable(details['ip']):
        bad_hosts.append(details['ip'])
    if details['aliases']:
        for host in details['aliases'].split(','):
            if host != 'retired' and not ping(host) and not http_reachable(host):
                bad_hosts.append(host)
    expiry = 0
    if 'cert' in details and details['cert']:
        hostinfo = get_certificate(details['cert'])
        if not hostinfo or not hostinfo.cert:
            print(f"Could not get cert for {details['cert']}")
            expiry = -2
        else:
            tfmt = "%Y-%m-%d %H:%M:%S"
            expires = hostinfo.cert.not_valid_after_utc.strftime(tfmt)
            if expires <= datetime.now(timezone.utc).strftime(tfmt):
                expiry = -1
            else:
                dtime = hostinfo.cert.not_valid_after_utc - datetime.now(timezone.utc)
                expiry = dtime.days
    retlist = [', '.join(bad_hosts) if bad_hosts else False, expiry]
    return retlist


def ping_servers(servers):
    ''' Ping servers
        Keyword arguments:
          servers: dictionary of server details
        Returns:
          results: dictionary of server details with status
    '''
    client = dask.distributed.Client(processes=True, n_workers=app.config["WORKERS"])
    results = {}
    try:
        futures = []
        for server_name, details in servers.items():
            future = client.submit(multiping, details)
            futures.append((server_name, details, future))
    except Exception as err:
        return inspect_error(err, 'Could not create futures')
    finally:
        print(f"Futures: {len(futures)}")
        for server_name, details, future in futures:
            try:
                is_not_reachable = future.result(timeout=app.config["TIMEOUT"])
                status = "Reachable" if not is_not_reachable else is_not_reachable
            except DaskTimeoutError:
                status = ["<span class='text-warning'>TIMEOUT</span>", 0]
            results[server_name] = {"description": details['description'],
                                    "ip": details['ip'],
                                    "canonical": details['canonical'],
                                    "aliases": details['aliases'],
                                    "type": details['type'],
                                    "suffix": details['suffix'],
                                    "status": status}
        client.close()
    return results

# ******************************************************************************
# * Navigation utility functions                                               *
# ******************************************************************************

def generate_navbar(active):
    ''' Generate the web navigation bar
        Keyword arguments:
          Navigation bar
    '''
    nav = '''
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
    '''
    for heading, subhead in NAV.items():
        basic = '<li class="nav-item active">' if heading == active else '<li class="nav-item">'
        drop = '<li class="nav-item dropdown active">' if heading == active \
               else '<li class="nav-item dropdown">'
        menuhead = '<a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" ' \
                   + 'role="button" data-toggle="dropdown" aria-haspopup="true" ' \
                   + f"aria-expanded=\"false\">{heading}</a><div class=\"dropdown-menu\" "\
                   + 'aria-labelledby="navbarDropdown">'
        if subhead:
            nav += drop + menuhead
            for itm, val in subhead.items():
                if itm == 'divider':
                    nav += "<div class='dropdown-divider'></div>"
                    continue
                link = f"/{val}" if val else ('/' + itm.replace(" ", "_")).lower()
                nav += f"<a class='dropdown-item' href='{link}'>{itm}</a>"
            nav += '</div></li>'
        else:
            nav += basic
            link = ('/' + heading.replace(" ", "_")).lower()
            nav += f"<a class='nav-link' href='{link}'>{heading}</a></li>"
    nav += '</ul></div></nav>'
    return nav

# ******************************************************************************
# * Payload utility functions                                                  *
# ******************************************************************************

def initialize_result():
    ''' Initialize the result dictionary
        Returns:
          decoded partially populated result dictionary
    '''
    result = {"rest": {"requester": request.remote_addr,
                       "authorized": False,
                       "url": request.url,
                       "endpoint": request.endpoint,
                       "error": False,
                       "elapsed_time": "",
                       "row_count": 0,
                       "pid": os.getpid()}}
    if app.config["LAST_TRANSACTION"]:
        print(f"Seconds since last transaction: {time() - app.config['LAST_TRANSACTION']}")
    app.config["LAST_TRANSACTION"] = time()
    if "Authorization" in request.headers:
        token = re.sub(r'Bearer\s+', "", request.headers["Authorization"])
        result['rest']['authorized'] = bool(token in app.config['KEYS'].values())
    return result


def generate_response(result):
    ''' Generate a response to a request
        Keyword arguments:
          result: result dictionary
        Returns:
          JSON response
    '''
    result["rest"]["elapsed_time"] = str(timedelta(seconds=time() - app.config["START_TIME"]))
    return jsonify(**result)

# *****************************************************************************
# * Documentation                                                             *
# *****************************************************************************

@app.route('/doc')
def get_doc_json():
    ''' Show documentation
    '''
    try:
        swag = swagger(app)
    except Exception as err:
        return inspect_error(err, 'Could not parse swag')
    swag['info']['version'] = __version__
    swag['info']['title'] = "Scientific Computing Software"
    return jsonify(swag)


@app.route('/help')
def show_swagger():
    ''' Show Swagger docs
    '''
    return render_template('swagger_ui.html')

# *****************************************************************************
# * Admin endpoints                                                           *
# *****************************************************************************

@app.route("/stats")
def stats():
    '''
    Show stats
    Show uptime/requests statistics
    ---
    tags:
      - Diagnostics
    responses:
      200:
        description: Stats
      400:
        description: Stats could not be calculated
    '''
    tbt = time() - app.config['LAST_TRANSACTION']
    result = initialize_result()
    start = datetime.fromtimestamp(app.config['START_TIME']).strftime('%Y-%m-%d %H:%M:%S')
    up_time = datetime.now() - app.config['STARTDT']
    result['stats'] = {"version": __version__,
                       "requests": app.config['COUNTER'],
                       "start_time": start,
                       "uptime": str(up_time),
                       "python": sys.version,
                       "pid": os.getpid(),
                       "endpoint_counts": app.config['ENDPOINTS'],
                       "time_since_last_transaction": tbt,
                      }
    return generate_response(result)

# ******************************************************************************
# * API endpoints                                                              *
# ******************************************************************************

@app.route('/')
@app.route('/home')
def show_home():
    ''' Home
    '''
    try:
        servers = find_servers()
    except Exception as err:
        return inspect_error(err, 'Could not find servers')
    results = ping_servers(servers)
    html = ""
    for stype in ['Physical', 'Virtual', 'Retired']:
        html += f"<h3>{stype} servers</h3>"
        html += f"<table id='{stype}' class='tablesorter standard'><thead><tr><th>Server</th>" \
                + "<th>Description</th><th>IP</th><th>Canonical</th>" \
                + "<th>Aliases</th><th>Status</th>"
        if stype != 'Retired':
            html += "<th>SSL expires</th>"
        html += "</tr></thead><tbody>"
        for key, val in results.items():
            if val['type'] != stype:
                continue
            url = f"{SCICOMP}/{val['suffix']}"
            val['aliases'] = " ".join(val['aliases'].split(',')) if val['aliases'] else ""
            if len(val['status']) == 2:
                status, cert = val['status']
            else:
                status = val['status']
                cert = ""
            if not cert:
                cert = ""
            else:
                if cert == -2:
                    cert = "Could not get cert"
                    color = 'red'
                elif cert == -1:
                    cert = "Expired"
                    color = 'red'
                elif cert < 30:
                    color = 'magenta'
                elif cert < 45:
                    color = 'orange'
                elif cert < 60:
                    color = 'yellow'
                elif cert < 90:
                    color = 'cyan'
                else:
                    color = 'lime'
                if isinstance(cert, int):
                    cert = f"{cert} day{'s' if cert != 1 else ''}"
                cert = f"<span style='color: {color}'>{cert}</span>"
            status = "<span class='text-success'>Reachable</span>" if not status \
                     else f"<span class='text-danger'>{status}</span>"
            if not val['description']:
                val['description'] = "<span style='color:red'>Need to set description</span>"
            html += f"<tr><td><a href='{url}' target='_blank'>{key}</a></td>" \
                    + f"<td width='300'>{val['description']}</td><td>{val['ip']}</td>" \
                    + f"<td>{val['canonical']}</td><td width='200'>{val['aliases']}</td>" \
                    + f"<td>{status}</td>"
            if stype != 'Retired':
                html += f"<td style='text-align:center'>{cert}</td>"
            html += "</tr>"
        html += "</tbody></table>"
    return make_response(render_template('home.html', urlroot=request.url_root,
                                         title="SCSW Servers", html=html,
                                         navbar=generate_navbar('Home'),
                                         servers=servers))


@app.route('/nutanix')
def show_nutanix():
    ''' Show Nutanix VMs
    '''
    try:
        servers = find_servers()
    except Exception as err:
        return inspect_error(err, 'Could not find servers')
    try:
        vms = get_nutanix()
    except Exception as err:
        return inspect_error(err, 'Could not get Nutanix VMs')
    html = "Descriptions are provided for servers that are on the " \
           + "<a href='https://wikis.janelia.org/display/ScientificComputing/SCSW+Servers'>" \
           + "SCSW Servers wiki page</a>."
    html += "<table id='nutanix' class='tablesorter standard'><thead><tr><th>Server</th>" \
            + "<th>Description</th><th>Alias</th><th>IP</th><th>CPUs</th>" \
            + "<th>Memory</th><th>Power</th></tr></thead><tbody>"
    for key, val in servers.items():
        name = val['canonical'].split('.')[0]
        if not name.startswith('vm') or name not in vms:
            continue
        if name in vms:
            vms[name]['description'] = val['description'] if val['description'] \
                                       else "<span style='color:red'>Need to set description</span>"
        else:
            vms[name]['description'] = ''
    for key, val in sorted(vms.items()):
        if 'description' not in val:
            val['description'] = ''
        color = 'lime' if val['power'] == 'on' else 'red'
        html += f"<tr><td>{key}</td><td>{val['description']}</td><td>{val['alias']}</td>" \
                + f"<td>{val['ip']}</td><td style='text-align:center'>{val['cpus']}</td>" \
                + f"<td style='text-align:center'>{humansize(val['memory'], places=0)}</td>" \
                + f"<td style='text-align:center'><span style='color: {color}'>" \
                + f"{val['power']}</span></td></tr>"
    html += "</tbody></table>"
    return make_response(render_template('home.html', urlroot=request.url_root,
                                         title="Nutanix VMs", html=html,
                                         navbar=generate_navbar('VMs'),
                                         servers=servers))

# *****************************************************************************

if __name__ == '__main__':
    if app.config["RUN_MODE"] == 'dev':
        app.run(debug=app.config["DEBUG"])
    else:
        app.run(debug=app.config["DEBUG"])
