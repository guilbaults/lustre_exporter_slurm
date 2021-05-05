import MySQLdb
import ldap
import base64
from aiohttp import web
import aiohttp
import asyncio
import re
import json
from cachetools import cached, LRUCache
import configparser
import sys

async def handle(request):
    server = request.match_info.get('server')
    async with aiohttp.ClientSession() as session:
        url = 'http://{}:9169/metrics'.format(server)
        async with session.get(url) as resp:
            metrics = await resp.text()
            text = str("\n".join(improve_metrics(metrics)))
    return web.Response(text=text)


@cached(cache=LRUCache(maxsize=100000))
def get_job_info(jobid):
    # Return the username and account used by this jobid
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    # Since the job table depends on how the slurm cluster was named, we need
    # to replace the name with format, cursor.execute cannot replace the
    # table name, only "normal" SQL parameters
    query_string = "select id_user,account from {} where id_job=%s".format(
        job_table)
    cursor.execute(query_string, (jobid,))
    result = cursor.fetchone()
    return {
        'user': get_username(result['id_user']),
        'account': result['account'],
    }


@cached(cache=LRUCache(maxsize=10000))
def get_username(uid):
    # Return the username, from a numerical uid
    if int(uid) == 0:
        return 'root'

    result = ldap_conn.search_s(
        'ou=People,dc=computecanada,dc=local',
        ldap.SCOPE_SUBTREE,
        '(uidNumber={})'.format(uid),
        ['uid'])
    return result[0][1]['uid'][0].decode('utf-8')


def improve_metrics(metrics):
    lines = []
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
                        labels_d.update(get_job_info(labels_d['jobid']))
                    except TypeError:
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
                    a.append('{}="{}"'.format(key, value))
                b = ",".join(a)
                lines.append(metric_name + "{" + b + "} " + metric_value)
            else:
                # cleanup_interval metric does not have a jobid
                lines.append(line)
        else:
            lines.append(line)
    return lines

if __name__ == '__main__':
    app = web.Application()
    app.add_routes([web.get('/{server}', handle)])

    config = configparser.ConfigParser()
    if len(sys.argv) > 1:
        config.read(sys.argv[1])
    else:
        config.read('config.ini')

    # autocommit need to be enabled since it affect SELECT queries, without
    # that setting, a snapshot of the table is taken when the script is
    # launched so no new jobs are seen.
    db = MySQLdb.connect(
        host=config.get('slurmdb', 'host'),
        port=int(config.get('slurmdb', 'port')),
        user=config.get('slurmdb', 'user'),
        password=config.get('slurmdb', 'password'),
        db=config.get('slurmdb', 'dbname'),
        autocommit=True)
    job_table = config.get('slurmdb', 'job_table')

    ldap_conn = ldap.initialize(config.get('ldap', 'server'))
    ldap_search_base = config.get('ldap', 'search_base')
    web.run_app(app, port=config.get('api', 'local_port'))
