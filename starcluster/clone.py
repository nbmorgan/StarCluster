import boto
import cluster
import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from pprint import pprint
import time
import json
from operator import itemgetter, attrgetter
import static

def store_config( cfg, cluster_template, cluster_tag ):
    table_region = cfg.aws['aws_config_table_region']
    table_name = cfg.aws['aws_config_table']
    if table_name is None:
        log.warning("AWS_CONFIG_TABLE is not defined. This cluster will not be clonable.")
    conn = boto.dynamodb2.connect_to_region(table_region)
    cluster_info_table = Table(table_name, connection=conn)
    cfg = StarClusterConfig().load()
    #pprint( cfg.clusters[cluster_template])
    my_cfg = cfg.clusters[cluster_template]
    my_cfg['cluster_tag'] = cluster_tag
    my_cfg['created_date'] = time.time()
    for key, value in my_cfg.iteritems():
        if isinstance(value, list) or isinstance(value, dict):
            my_cfg[key] = json.dumps(value)
        if value is None:
            my_cfg[key] = 'None'
    if '__name__' in my_cfg:
        my_cfg['name'] = my_cfg['__name__']
    my_cfg.pop('name')
    cfg_item = Item(cluster_info_table, data=my_cfg)
    cfg_item.save()
    cfg = StarClusterConfig().load()
    return cfg.clusters[cluster_template]

def load_config( cfg, cluster_tag):
    table_region = cfg.aws['aws_config_table_region']
    conn = boto.dynamodb2.connect_to_region(table_region)
    table_name = cfg.aws['aws_config_table']
    cluster_info_table = Table(table_name, connection=conn)
    config = cluster_info_table.query(cluster_tag__eq=cluster_tag, )
    my_list = []
    for a in  config:
        my_list.append({})
        for k in a.keys():
            try:
                my_list[-1][ k ] =  json.loads(a[k])
            except (ValueError, TypeError):
                if k == 'name':
                    my_list[-1]['__name__'] = a[k]
                elif isinstance(a[ k ], basestring) and a[ k ] == 'None':
                    my_list[-1][ k ] = None
                else:
                    for s in [static.CLUSTER_SETTINGS, 
                            static.KEY_SETTINGS, static.PLUGIN_SETTINGS]:
                        if k in s and not isinstance(s[k][0](), list):
                            my_list[-1][ k ] = s[k][0](a[k])
                            
                    if k not in my_list[-1]:
                        my_list[-1][ k ] = a[k]
            
    res = sorted(my_list, key=lambda x: x['created_date'])
    res[-1].pop('created_date')
    if len(res) > 0:
        return res[-1]
    else:
        return None

if __name__ == "__main__":
    from starcluster.config import StarClusterConfig
    cfg = StarClusterConfig().load()
    for cl in cfg.clusters.keys():
        sent = store_config(cfg,cl, cl +'_test')
        res = load_config(cfg, cl +'_test')
        for k in sent.keys():
            if sent[k] != res[k]:
                print cl
                print k
                pprint(sent[k])
                pprint(res[k])
    print sent == res
    """
    for k, v in res.iteritems():
        print '[',k,']'
        pprint(v)
        pprint(sent[k])"""

