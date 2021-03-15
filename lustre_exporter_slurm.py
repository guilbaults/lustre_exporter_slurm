import base64
import paramiko
import csv
from aiohttp import web
import aiohttp
import asyncio
import re
import json
from cachetools import cached, LRUCache, TTLCache
import configparser
import sys

async def handle(request):
    server = request.match_info.get('server')
    async with aiohttp.ClientSession() as session:
        async with session.get('http://{}:9169/metrics'.format(server)) as resp:
            metrics = await resp.text()
            text = str("\n".join(improve_metrics(metrics)))
    return web.Response(text=text)

app = web.Application()
app.add_routes([web.get('/{server}', handle)])

@cached(cache=TTLCache(maxsize=10000, ttl=60))
def get_jobs_info():
    try:
        stdin, stdout, stderr = client.exec_command('/opt/software/slurm/bin/squeue -t r --format %A,%u,%a')
        reader = csv.reader(stdout)
        jobs = {}
        for row in reader:
            jobs[row[0]] = {'user': row[1], 'account': row[2]}
        return jobs
    except paramiko.ssh_exception.SSHException as err:
        print(err)
        sys.exit(1)

@cached(cache=LRUCache(maxsize=10000))
def get_username(uid):
    try:
        stdin, stdout, stderr = client.exec_command('/usr/bin/id --name --user {}'.format(uid))
        return stdout.read().strip().decode('UTF-8')
    except paramiko.ssh_exception.SSHException as err:
        print(err)
        sys.exit(1)

def improve_metrics(metrics):
    lines = []
    jobs_info = get_jobs_info()
    for line in metrics.splitlines():
        # On each metric line, extract the name, labels and value
        m1 = re.match(r'(lustre_job.*){(.*)} (.*)', line)
        if m1:
            metric_name = m1.group(1)
            labels = m1.group(2).split(',')
            metric_value = m1.group(3)

            labels_d = {}
            # Recreate the labels so we print all of them again
            for label in labels:
                # Grab the existing label from the metric
                m2 = re.match(r'(\w+)="(.+)"', label)
                if m2:
                    labels_d[m2.group(1)] = m2.group(2)

            # Add the FS name as a label to simplify aggregation queries
            labels_d['fs'] = labels_d['target'].split('-')[0]

            if 'jobid' in labels_d:
                if labels_d['jobid'].isnumeric():
                    # slurm jobid
                    try:
                        labels_d.update(jobs_info[labels_d['jobid']])
                    except KeyError:
                        pass
                else:
                    # login node or mgmt node with procname.uid
                    try:
                        m3 = re.match(r'(.*)\.(\d+)', labels_d['jobid'])
                        labels_d['application'] = m3.group(1)
                        labels_d['user'] = get_username(m3.group(2))
                    except:
                        pass
                # Repackage the new labels
                a = []
                for key, value in labels_d.items():
                    a.append('{}="{}"'.format(key,value))
                b = ",".join(a)
                lines.append(metric_name + "{" + b + "} " + metric_value)
            else:
                # cleanup_interval metric does not have a jobid
                lines.append(line)
        else:
            lines.append(line)
    return lines

if __name__ == '__main__':
    config = configparser.ConfigParser()
    if len(sys.argv) > 1:
        config.read(sys.argv[1])
    else:
        config.read('config.ini')

    private_key = paramiko.RSAKey.from_private_key_file(config.get('ssh', 'private_key'))
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.connect(config.get('ssh', 'host'), username=config.get('ssh', 'user'), pkey=private_key)

    web.run_app(app, port=config.get('api', 'local_port'))
    client.close()
