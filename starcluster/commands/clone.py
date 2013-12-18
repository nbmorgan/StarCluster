import boto
from boto import dynamodb2
from boto.dynamodb2.table import Table
from boto.s3.key import Key
from pprint import pprint
import time
import json
import hashlib
from starcluster.logger import log, console
from starcluster.config import StarClusterConfig
import re
from starcluster.commands.completers import ClusterCompleter 
import os.path
from starcluster.config import StarClusterConfig
from starcluster.cluster import ClusterManager
from starcluster import static
from pprint import pprint
from boto.exception import EC2ResponseError
from boto.ec2.connection import EC2Connection
import boto.ec2
import time
from datetime import datetime, timedelta
import dateutil.parser
import re

from starcluster import static

class CmdClone(ClusterCompleter):
    """
    clone f <from_tag> -t <from_template_name> -a <availability_zone> 

    Clone an existing cluster <from_tag> (or template description<from_template_name)
    into new availability zone <availability_zone>

    Example:
        $starcluster clone -f somecluster -a us-west-1a
        or
        $starcluster clone -t sometemplate -a us-west-1b

    This will launch a cluster somecluster-west in us-west-1 that uses copies
    of any volumes and amis that belong to somecluster
    """

    names=['clone']
    def addopts(self, parser):
        parser.add_option( '-f', '--from-tag', dest="cluster_tag", 
                        action="store", type="string", default=None, 
                        help="The name of the cluster to be cloned" )
        parser.add_option( '-t', '--from-template', dest="template",
                        action="store", type="string", default=None,
                        help="The name of a template to clone")
        parser.add_option( '-a', '--availability-zone', dest="zone",
                        action="store", type="string", default=None,
                        help=("The availability zone into which this cluster"
                                " should be cloned"))
    def execute(self, args):
        if not self.opts.zone:
            self.parser.error(("Please specify an availability zone in which"
                                " to clone"))
        if not self.opts.cluster_tag and not self.opts.template:
            self.parser.error(("Please specify either a cluster tag/name (-f)"
                                " or a template (-t) to clone"))
        if self.opts.cluster_tag and self.opts.template:
            self.parser.error(("Please specify EITHER the cluster tag(name) or"
                                " a template to clone. Cannot have both."))
        cloneCluster(self.opts.zone, cluster_tag=self.opts.cluster_tag,
                        template=self.opts.template)

def store_config( cluster_template, cluster_tag, cluster_region, 
                    template_name=None, zone=None ):
    """
    Stores json encoded cluster_template used to start up <cluster tag>
    in <cluster_regioHHn>.
    <zone> is zone name if cluster is tied to a zone
    <cluster_region> in s3.

    Pointer to template is stored in dynamo.
    """
    cfg = StarClusterConfig().load()
    s3_bucket = cfg.aws['aws_meta_bucket']
    table_name = cfg.aws['aws_config_table']
    if table_name is None:
        log.warning(("AWS_CONFIG_TABLE is not defined."
                " This cluster will not be cloneable."))
        return False
    if s3_bucket is None:
        log.warning(("AWS_META_BUCKET is not defined."
                "This cluster will not be cloneable"))
        return False
    conn = boto.dynamodb2.connect_to_region('us-east-1')
    cluster_info_table = Table(table_name, connection=conn)
    if cluster_info_table is None:
        return log.warning("%s table not found in us-east-1" % table_name)
    config_path = config_to_s3(json.dumps( cluster_template ), 
        s3_bucket )
    #data to be stored on dyname
    table_data = {}
    table_data['cluster_tag'] = cluster_tag
    table_data['template_name'] = template_name if template_name else 'NA'
    table_data['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S +0000",
                                    time.gmtime())
    table_data['config_path'] = config_path
    table_data['region'] = cluster_region
    table_data['zone'] = zone.name if zone else 'NA'
    cluster_info_table.put_item(data=table_data)
    return True 

def config_to_s3( js_cfg, s3_bucket ):
    conn = boto.connect_s3()
    bucket = conn.create_bucket( s3_bucket )
    m = hashlib.md5()
    m.update(js_cfg)
    md5_js_cfg = m.hexdigest()
    k = Key(bucket)
    k.key = 'sc_config/' + md5_js_cfg
    k.set_contents_from_string(js_cfg)
    return 's3://%s/%s' % (s3_bucket, k.key)

def get_dynamo_table( table_name ):
    connection = boto.dynamodb2.connect_to_region('us-east-1')
    tables = connection.list_tables()
    if table_name in tables['TableNames']:
        #pprint(connection.describe_table( table_name )['Table'])
        return Table( table_name )
    else:
        log.info("%s not found, unable to clone this cluster" % table_name)
        return None

def get_config( cluster_tag=None, template_name=None ):
    """
    Loads a list of configurations started with the given <cluster_tag>,
    most recent configuration is first
    """
    cfg = StarClusterConfig().load()
    if not cluster_tag and not template_name:
        log.warning("Attempt to clone without a template_name or cluster_tag")
        return []
    s3_bucket = cfg.aws['aws_meta_bucket']
    table_name = cfg.aws['aws_config_table']
    if table_name is None:
        log.warning(("AWS_CONFIG_TABLE is not defined."
                        " This cluster will not be cloneable."))
        return False
    if s3_bucket is None:
        log.warning(("AWS_META_BUCKET is not defined."
                        "This cluster will not be cloneable"))
        return False
    conn = boto.dynamodb2.connect_to_region('us-east-1')
    cluster_info_table = Table(table_name, connection=conn)
    #print cluster_info_table.describe()
    if cluster_tag:
        clusters = cluster_info_table.query( cluster_tag__eq=cluster_tag)
    else:
        clusters = cluster_info_table.scan( template_name__eq=template_name )
    if clusters:
        return [c for c in clusters]
    else:
        return []

def load_config( config_path ):
    """
    Loads a json object at given path on s3, object should be config for 
    a cluster

    config_path should be of form s3://bucket/dir/file or 
        s3://bucket/file
    """
    match = re.match(r's3://([^/]+)/(.+)', config_path)
    s3_bucket = match.group(1)
    obj = match.group(2)
    conn = boto.connect_s3()
    bucket = conn.create_bucket( s3_bucket )
    k = Key(bucket)
    k.key = obj
    return json.loads(k.get_contents_as_string())

def cpVolume(volume_id, region_to, zone_to, region_from=None):
    if region_from is None:
        region_from = region_to
    conn = boto.ec2.connect_to_region(region_from)
    volumes = conn.get_all_volumes([volume_id])
    description = 'Created for copy of %s from %s to %s on %s' % (volume_id, 
                    region_from, zone_to, datetime.today().isoformat(' ') )
    log.info( "Copy Volume init: %s")
    vol = volumes[0]
    snap = vol.create_snapshot( description )
    while snap.status != u'completed':
        time.sleep(30)
        snap.update()
        log.info( "Snapshot status: %s"  % snap.status)
    if 'Name' in vol.tags:
        snap.add_tag('Name', vol.tags['Name'] + ' copy')
    snap.add_tag('Source Volume', vol.id)
    snap.add_tag('Source Zone', vol.type)
    if region_from != region_to:
        conn = boto.ec2.connect_to_region(region_to)
    new_snap_id = conn.copy_snapshot( source_region=region_from, 
                    source_snapshot_id=snap.id, description=description)
    new_snap = conn.get_all_snapshots( [new_snap_id])[0]
    while new_snap.status != u'completed':
        time.sleep(30)
        new_snap.update()
        log.info( "New Snapshot Status: %s" % new_snap.status)
    if 'Name' in vol.tags:
        new_snap.add_tag('Name', vol.tags['Name'] + ' copy')
    new_snap.add_tag('Source ID',snap.id)
    new_snap.add_tag('Source Region', region_from)
    log.info( "Creating new volume" )
    new_vol = new_snap.create_volume(zone_to, size=vol.size, 
                                    volume_type=vol.type, iops=vol.iops)
    while new_vol.status != u'available':
        time.sleep(30)
        log.inf("New Volume Status %s" % new_vol.update())
    if 'Name' in vol.tags:
        new_vol.add_tag('Name',vol.tags['Name'][:10] + '-' + zone_to)
    new_vol.add_tag('Source ID', vol.id)
    new_vol.add_tag('Source Zone', vol.zone)
    return new_vol.id

def cpAMI( dest_region, source_region, source_image_id):
    if source_region == dest_region:
        log.info(("Source ami in same region as destination."
                    " Not copying ami"))
        return  source_image_id
    image = amiExists(dest_region, source_image_id)
    if image is not None:
        log.info( "AMI or copy exists in %s [%s]" % (dest_region, image.id) )
        return image.id
    conn=boto.ec2.connect_to_region(source_region)
    image = conn.get_all_images([source_image_id])[0]
    conn = boto.ec2.connect_to_region(dest_region)
    name = image.name[:20] + " copy"
    description = "Copy of %s from region %s on %s" % (source_image_id, source_region,  datetime.today().isoformat(' '))
    log.info("Copying %s from %s to %s" %  (source_image_id, source_region, dest_region) )
    res = conn.copy_image(source_region=source_region, source_image_id=source_image_id, name=name, description=description)
    log.info( "Created Image:%s"%res.image_id)
    image = conn.get_all_images([res.image_id])[0]
    while image.update() != 'available':
        time.sleep(30)
        log.info( "AMI creation state: %s" % image.state)
    log.info( "%s[%s] ready" % (name, res.image_id))
    image.add_tag('Source AMI', source_image_id)
    image.add_tag('Source Region', source_region)
    image.update()
    return image.id

def amiExists( dest_region, source_image_id ):
    conn=boto.ec2.connect_to_region(dest_region)
    if conn is None:
        raise Exception('Region %s, not found' % dest_region)
    images = conn.get_all_images(owners=['self'])
    for image in images:
        if 'Source AMI' in image.tags and \
                    image.tags['Source AMI'] == source_image_id:
            log.info( "AMI source copy already exists in this region" )
            return image
    return None

def getSpotPrices(instance_type, max_times=10):
    result = {}
    for r in boto.ec2.regions():
        try:
            conn = boto.ec2.connect_to_region(r.name)
            for z in  conn.get_all_zones():
                end = datetime.utcnow().isoformat()
                start = (datetime.utcnow() - timedelta(days=1)).isoformat()
                sph =  conn.get_spot_price_history(start_time=start,
                                                instance_type=instance_type, 
                                                availability_zone=z.name)
                if len(sph) > 0:
                    sph = sph[:max_times]
                    final =  dateutil.parser.parse(sph[0].timestamp)
                    hdr = []
                    cost = []    
                    for s in sph:
                        a = dateutil.parser.parse(s.timestamp)
                        if final-a < timedelta(days=1):
                            hdr.append(str((final - a)).split(':')[0] +':' + 
                                                str((final - a)).split(':')[1])
                            cost.append(str(s.price))
                    result[z.name] = {}
                    result[z.name]['print'] = '\n'.join([z.name, 
                                '\t'+ '\t'.join(hdr), '\t'+ '\t'.join(cost)])
                    result[z.name]['current'] = float(cost[0])
        except EC2ResponseError as e:
            pass
    return result  

def checkPrices(ctype=None, cluster_tag=None, max_times=10):
    """
    Checks the spot prices
    no args compares currently running clusters to possible spot prices
    ctype checks for a certain type of image i.e. 'm1.xlarge' and returns 
    prices in order of cheapest to most expensive
    cluster_tag checks only for one running cluster
    """
    assert ctype is None or cluster_tag is None, ("you cannot specify a "
                                    "cluster and an image type.")
    if ctype==None:
        cfg = StarClusterConfig().load()
        cm = ClusterManager(cfg)
        for cluster in cm.get_clusters():
            if cluster_tag is not None and  cluster.cluster_tag != cluster_tag:
                continue
            region, template = readLog(tag = cluster.cluster_tag)
            t = "%s - of type(%s) in zone(%s) with bid(%s)"% (
                    cluster.cluster_tag,  cluster.master_instance_type,
                    cluster._nodes[0].placement,cluster.spot_bid)
            print "="*len(t)
            print t
            sp = getSpotPrices( cluster.master_instance_type, 
                                    max_times=max_times )
            print sp[cluster._nodes[0].placement]['print']
            print "="*len(t)
            print 
            print
            print "Better/equal prices @"
            curr = sp[cluster._nodes[0].placement]['current']
            for k,v in sp.iteritems():
                if curr >= v['current'] and k != cluster._nodes[0].placement:
                    print v['print']
    else:
        sp = getSpotPrices( ctype, max_times=max_times )
        a = [(p['current'],k) for k,p in sp.iteritems()]
        a.sort()
        print "type(%s)  from cheapest to most expensive"%ctype
        for _,k in a:
            print sp[k]['print']

def getSpotStatus():
    ignore_states = ['cancelled', 'active']
    for r in boto.ec2.regions():
        if r.name != 'us-gov-west-1':
            conn = boto.ec2.connect_to_region(r.name)
            sir = conn.get_all_spot_instance_requests()
            if len(sir) > 0:
                print "Spot Instance requests in ",r.name
            running = []
            cancelled = []
            for s in sir:
                ls = s.launch_specification
                if s.state not in ignore_states:
                    print "\tpending id:",s.id
                    print "\t\tstate:",s.state
                    print "\t\tstatus:",s.status.code
                    print "\t\tzone:",ls.placement
                    print "\t\ttype:", ls.instance_type
                elif s.state=='active':
                    running.append(s.id+'('+ls.instance_type+
                                                ')['+ls.placement[-2:]+']' )
                elif s.state=='cancelled':
                        cancelled.append(s.id+'('+ls.instance_type+
                                                ')['+ls.placement[-2:]+']' )
            if len(running) > 0:
               print "\trunning instances:", ', '.join(running)
            if len(cancelled) > 0:
               print "\tcancelled instances:", ', '.join(cancelled)

def cloneCluster(new_zone, cluster_tag=None, template=None):
    assert cluster_tag is not None or template is not None, \
            "You must provide a cluster template or a cluster tag"
    #get full config
    cfg = StarClusterConfig().load()
    cm = ClusterManager(cfg)
    if new_zone[-1] not in ['a','b','c','d']:
        raise Exception("%s does not appear to be a ZONE" % new_zone)
    dest_region = new_zone[:-1]
    if cluster_tag is not None:
        lookup = get_config( cluster_tag=cluster_tag )
        if lookup:
            template = lookup[0]['template_name']
            cluster_cfg = load_config(lookup[0]['config_path'])
        else:
            raise Exception("Unable to find cluster tag[%s]" % cluster_tag)
    else:
        lookup = get_config( template_name=template )
        if lookup:
            template = lookup[0]['template_name']
            cluster_cfg = load_config(lookup[0]['config_path'])
        else:
            raise Exception("Unable to find template[%s]" % cluster_tag)
    #get autogenerated portion of config
    auto_config = ConfigParser.RawConfigParser()
    auto_config.read(os.path.join(
                        static.STARCLUSTER_CFG_DIR, 'cfgs/auto-gen.cfg'))
    new_cluster_sect = []
    new_cluster_sect.append(('KEYNAME', static.KEYNAMES[dest_region]))
    new_cluster_sect.append(('EXTENDS', template))
    new_volume_sect = []
    nit_v = ''
    if 'node_instance_types' in cluster_cfg:
        for nit in cluster_cfg['node_instance_types']:
            source_region = regionFromAMI(nit['image'])
            new_ami = cpAMI( dest_region=dest_region, 
                        source_region=source_region,
                        source_image_id=nit['image'])
            if len(nit_v) > 0:
                nit_v += ', '
            if 'type' in nit:
                nit_v += ':'.join([nit['type'], new_ami, str(nit["size"])])
            else:
                nit_v += new_ami
    if 'node_instance_type' in cluster_cfg \
            and cluster_cfg['node_instance_type']:
        if len(nit_v)>0:
            nit_v += ', '
        nit_v += cluster_cfg['node_instance_type']
        new_cluster_sect.append(('NODE_INSTANCE_TYPE', nit_v))
    print cluster_cfg
    if 'master_instance_type' in cluster_cfg \
                                    and cluster_cfg['master_instance_type']:
        new_cluster_sect.append(('MASTER_INSTANCE_TYPE',
                                        cluster_cfg['master_instance_type']) )
    if 'master_image_id' in cluster_cfg and cluster_cfg['master_image_id']:
        source_region = regionFromAMI(cluster_cfg['master_image_id'])
        new_ami =  cpAMI( dest_region=dest_region, 
                        source_region=source_region,
                        source_image_id=cluster_cfg['master_image_id'])
        new_cluster_sect.append(('MASTER_IMAGE_ID', new_ami))

    if 'node_image_id' in cluster_cfg and cluster_cfg['node_image_id']:
        source_region = regionFromAMI(cluster_cfg['node_image_id'])
        new_ami =  cpAMI( dest_region=dest_region, 
                        source_region=source_region,
                        source_image_id=cluster_cfg['node_image_id'])
        new_cluster_sect.append(('NODE_IMAGE_ID', new_ami))
    volumes_names = []
    if 'volumes' in cluster_cfg:
        for vname, vdesc in cluster_cfg['volumes'].iteritems():
            new_volume_sect = []
            vname = removeRegion(vname)
            volume_sec_name = 'volume %s-%s' % (vname, new_zone)
            counter = 0
            while auto_config.has_section(volume_sec_name):
                volume_sec_name = 'volume %s-%s-%02d' % (vname, new_zone,counter)
                counter += 1
            region_from, zone_from = regionFromVolume( vdesc['volume_id'] )
            if region_from is None:
                raise Exception("Cannot find %s  in any region." % v)
            new_vol_id = cpVolume(vdesc['volume_id'], new_zone[:-1], 
                                            new_zone, region_from=region_from)
            new_volume_sect.append(('VOLUME_ID', new_vol_id))
            new_volume_sect.append(('MOUNT_PATH', vdesc['mount_path']))
            auto_config.add_section(volume_sec_name)
            for k,v in  new_volume_sect:
                auto_config.set(volume_sec_name, k,v)
            volumes_names.append( volume_sec_name.split(' ')[1])
    if len(volumes_names) > 0:
        new_cluster_sect.append( ('VOLUMES', ','.join(volumes_names) ) )
    counter = 0
    template = removeRegion(template)
    cluster_sec_name = 'cluster %s-%s' % (template, new_zone)
    while auto_config.has_section(cluster_sec_name):
        cluster_sec_name = 'cluster %s-%s-%02d' % (template, new_zone, counter)
        counter += 1
    auto_config.add_section(cluster_sec_name)
    for k,v in  new_cluster_sect:
        auto_config.set(cluster_sec_name, k,v)
    with open(os.path.join( static.STARCLUSTER_CFG_DIR, 'cfgs/auto-gen.cfg'), 
            'wb') as configfile:
        auto_config.write(configfile)
    log.info( ("starcluster -r %s start --force-spot-master -b "
                "<my-bid> -c %s my-cluster")
                % (new_zone[:-1], cluster_sec_name.split(' ')[1]))

def getClusterConfig(cluster_template, cfg):
    cluster_sect = cfg._config.items('cluster %s' % cluster_template)
    for name, value in cluster_sect:
        if name == 'volumes':
            volume_sect = cfg._config.items('volume %s'% value)
    return cluster_sect, volume_sect

def removeRegion( astring):
    for r in boto.ec2.regions():
        if r.name != 'us-gov-west-1':
            conn = boto.ec2.connect_to_region(r.name)
            for z in conn.get_all_zones():
                new_string = re.sub( r'-?'+z.name+'-?', '', astring)
                if len(new_string) < len(astring):
                    return new_string
            new_string = re.sub( r'-?'+r.name+'-?', '', astring)
            if len(new_string) < len(astring):
                return new_string
    return astring 

def regionFromAMI( ami_id ):
    for r in boto.ec2.regions():
        if r.name != 'us-gov-west-1':
            conn = boto.ec2.connect_to_region(r.name)
            try:
                image = conn.get_all_images([ami_id])
                if len(image) > 0:
                    return r.name
            except:
                pass
    return None

def regionFromVolume( volume_id ):
    for r in boto.ec2.regions():
        if r.name != 'us-gov-west-1':
            conn = boto.ec2.connect_to_region(r.name)
            try:
                volumes = conn.get_all_volumes([volume_id])
                if len(volumes) > 0:
                    return r.name, volumes[0].zone
            except:
                pass
    return None,None

if __name__ == "__main__":
    from starcluster.config import StarClusterConfig
    log.addHandler(console)
    cloneCluster('us-west-2a', template='heterocluster')
