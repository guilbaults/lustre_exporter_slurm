#lustre\_exporter\_slurm

This script is intended to be used with [Lustre jobstats](http://doc.lustre.org/lustre_manual.xhtml#dbdoclet.jobstats) and [lustre_exporter](https://github.com/HewlettPackard/lustre_exporter). The REST requests from prometheus to lustre\_exporter will be modified by this script to add user and account labels instead of only `$SLURM_JOB_ID`. 

Metrics from nodes using `procname_uid` will also be modified, the numerical UID will be extracted and converted to a username and stored in the user label.

A SSH connection to a node with `squeue` configured is required, this could also run locally if required if the node have access to `squeue`.

## Prometheus config
Relabel is used to redirect the REST call to the local lustre\_exporter\_slurm script instead of pooling directly the MDS/OSS. 

```
  relabel_configs:
    - source_labels: [__address__]
      target_label: __metrics_path__
      regex: '(.*):(.*)'
      replacement: '/$1'
    - source_labels: [__address__]
      target_label: instance
    - source_labels: [__address__]
      regex: '(.*):(.*)'
      replacement: '127.0.0.1:8080'
      target_label: __address__
``` 