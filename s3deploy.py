import sys
from boto.s3.connection import S3Connection
from boto.ec2.autoscale import AutoScaleConnection
from boto3.session import Session
import boto.ec2
import boto
import boto3
import os
import subprocess
import random
import time
import socket

count_sc=0

proc_init = subprocess.Popen('ps aux | grep "python"', shell=True,stdout=subprocess.PIPE)
(out_init, err) = proc_init.communicate()
for el in out_init.split(os.linesep):
    if "autoscale_monitor" in el:
        count_sc=count_sc+1

if count_sc >= 2:
    pass
else:
    try: 
        os.remove('/var/run/autoscale_monitor.lock')
    except:
        pass
try:
    os.open("/var/run/autoscale_monitor.lock", os.O_CREAT|os.O_EXCL)
except:
   sys.exit(1)
standby_count = 0
autoscale_icount = 0
retry = 0
try:
    current_revid_adserver = os.path.realpath('/opt/adaptv/new_adserver').split('.')[1] 
    current_revid_shortribs = os.path.realpath('/opt/adaptv/shortribs').split('.')[1]
    current_revid_geoip = os.path.realpath('/opt/adaptv/geoip').split('.')[1]
except:
    print "No files in /opt/adaptv/"
    os.remove('/var/run/autoscale_monitor.lock')
    sys.exit(1)
# Pulling AWS Meta data
try:
    proc = subprocess.Popen(['curl', '-s', 'http://169.254.169.254/latest/meta-data/instance-id'], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    proc = subprocess.Popen(['curl', '-s', 'http://169.254.169.254/latest/meta-data/instance-type'], stdout=subprocess.PIPE)
    (out1, err) = proc.communicate()
    proc = subprocess.Popen(['curl', '-s', 'http://169.254.169.254/latest/meta-data/placement/availability-zone'], stdout=subprocess.PIPE)
    (out2, err) = proc.communicate()
    instancetype = out1
    instanceid = out
    region = out2[:-1]
    zone = out2[-1:]
except:
    print "UNABLE TO FETCH AWS METADATA"
    os.remove('/var/run/autoscale_monitor.lock')
    sys.exit(1)


client = boto3.client('autoscaling', region_name=region, api_version=None, use_ssl=True, verify=None, endpoint_url=None, aws_access_key_id='AKIAJCSXPE5DQXS67YOQ', aws_secret_access_key='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', aws_session_token=None, config=None)
try:
    conn = S3Connection('AKIAJCSXPE5DQXS67YOQ','CjEu+AUD5B03rH84YULKhI3eZ6/1AbiHeRFbbQ5h')
    ec2_connection = boto.connect_ec2('AKIAJCSXPE5DQXS67YOQ','CjEu+AUD5B03rH84YULKhI3eZ6/1AbiHeRFbbQ5h')
    auto_conn = AutoScaleConnection('AKIAJCSXPE5DQXS67YOQ', 'CjEu+AUD5B03rH84YULKhI3eZ6/1AbiHeRFbbQ5h')
    bucket = conn.get_bucket('atv.adserver.autoscaling.'+region)
except:
    print "UNABLE TO CONNECT TO S3"
    os.remove('/var/run/autoscale_monitor.lock')
    sys.exit(1)    

#bucket = conn.get_bucket('atv.adserver.autoscaling.'+region)


#def check_standby():
#    global instanceid
#    print instanceid
#    l = client.describe_auto_scaling_instances(
#    InstanceIds=[
#        instanceid,
#    ],
#    MaxRecords=1,
#    )
#    print l['AutoScalingInstances'][0]['LifecycleState']
#    if l['AutoScalingInstances'][0]['LifecycleState'] ==  'Standby' or l['AutoScalingInstances'][0]['LifecycleState'] ==  'Pending':
#        os.remove('lock')
#        sys.exit(1)
#    check_deploy_adserver()

def check_deploy_adserver():
    try:
        for key in bucket.list():
            directory = key.name.encode('utf-8').split('/')
            filen = directory[-1]
            if 'new_adserver' in filen and '14.04' in filen:
                print 'Adserver REVID in S3:'
                f= filen.split('.')[1]
                print f
        path =  os.path.realpath('/opt/adaptv/new_adserver')
        print path.split('.')[1]
        print('\n')
    
        if f == path.split('.')[1]:
            print 'ADSERVER REVID does match'
    
        else:
            check_pool('adserver',f)
    except:
        pass

def check_deploy_shortribs():
    try:
        for key in bucket.list():
            directory = key.name.encode('utf-8').split('/')
            filen = directory[-1]
            if 'shortribs' in filen and 'counter' not in filen and '14.04' in filen:
                print 'Shortribs REVID in S3:'
                f1= filen.split('.')[1]
                print f1
        path =  os.path.realpath('/opt/adaptv/shortribs')
        print path.split('.')[1]
        print('\n')

        if f1 == path.split('.')[1]:
            print 'Shortribs REVID does match'
        else:
            check_pool('shortribs',f1)
    except:
        pass

def check_deploy_geoip():
    try:
	for key in bucket.list():
            directory = key.name.encode('utf-8').split('/')
            filen = directory[-1]
            if 'geoip' in filen and '14.04' in filen:
                print 'Geoip REVID in S3:'
                f3= filen.split('.')[1]
                print f3
        path =  os.path.realpath('/opt/adaptv/geoip')
        print path.split('.')[1]
        print('\n')

        if f3 == path.split('.')[1]:
            print 'Geoip REVID does match'

        else:
            check_pool('geoip',f3)
    except:
        pass


def check_pool(item,revid):
    try:
        global standby_count,autoscale_icount
        for i in auto_conn.get_all_autoscaling_instances():
            autoscale_icount = autoscale_icount+1
            if i.lifecycle_state == 'Standby':
                standby_count  =  standby_count+1
        print standby_count
        print autoscale_icount
        if autoscale_icount >= 10:
            if standby_count < (autoscale_icount)/5 or standby_count == 0 or autoscale_icount == 1:
                sleepmin = (random.randint(1,6)*60)
                time.sleep(sleepmin)
                deploy_start(item,revid)
            else:
                autoscale_icount = 0
                standby_count =0 
                print 'Standby Count > 0 and > 20 percent of Autoscale count'
                pass
        else:
            sleepmin = (random.randint(1,6)*60)
            time.sleep(sleepmin)
            deploy_start(item,revid)
    except:
        pass


def deploy_start(item,revid):
    global instanceid
    print 'deploy'
    try:
        client.enter_standby(
            InstanceIds=[
                instanceid,
            ],
            AutoScalingGroupName='adserver-'+zone+'-'+instancetype,
            ShouldDecrementDesiredCapacity=False
    )
    except:
        print 'already in standby'
        pass
    try: 
        while ((client.describe_auto_scaling_instances(
        InstanceIds=[
            instanceid,
        ],
        MaxRecords=1,
        )['AutoScalingInstances'][0]['LifecycleState']) !=  'Standby'):
            time.sleep(10)
        if item == 'adserver':
            deploy_adserver(revid)
        elif item == 'geoip':
            deploy_geoip(revid)
        elif item == 'shortribs':
            deploy_shortribs(revid)
    except:
        exit_deploy()
        pass

def deploy_adserver(revid):
    try:
        global current_revid_adserver,retry
        p1 = subprocess.Popen('sudo -Hu adaptv /opt/adaptv/new_adserver/adserver stop', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex1 = p1.wait()
        p5 = subprocess.Popen('sudo -Hu adaptv s3cmd --skip-existing get s3://atv.adserver.autoscaling.'+region+'/adserver/new_adserver*14.04.tgz /opt/adaptv/', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True)
        e5 = p5.wait()
        p8 = subprocess.Popen('sudo -Hu adaptv s3cmd --skip-existing get s3://atv.adserver.autoscaling.'+region+'/adserver/'+region+zone+'/server.conf /opt/adaptv/etc/adserver/', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex8 = p8.wait()
        p9 = subprocess.Popen('sudo -Hu adaptv echo "auto_scaling_server=1" >> /opt/adaptv/etc/adserver/server.conf', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex9 = p9.wait()
        server_file = open('/opt/adaptv/etc/adserver/server.conf','r')
        server_conf_lines = server_file.readlines()
        server_file.close()
        server_file = open('/opt/adaptv/etc/adserver/server.conf','w')
        for line in server_conf_lines:
            if "num_threads" not in line:
                server_file.write(line)
            if "ssc_server" in line and region =='us-east-1':
                ssc_ec = socket.gethostbyaddr('mtc-nj-ssc'+str(random.randint(1,40))+'.cl.adap.tv')[2][0]
	        server_file.write('ssc_server='+ssc_ec+':10000')
		print 'chose ssc:'+ssc_ec
            if "ssc_server" in line and region=='us-west-1':
	        ssc_wc = socket.gethostbyaddr('ssc'+str(random.randint(1,40))+'.sj.adap.tv')[2][0]
		server_file.write('ssc_server='+ssc_wc+':10000')
		print 'chose ssc:'+ssc_wc
        server_file.close()
        p2 = subprocess.Popen('sudo -Hu adaptv tar -zxf /opt/adaptv/new_adserver.'+revid+'*.tgz -C /opt/adaptv/', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex2 = p2.wait()
        p3 = subprocess.Popen("sed -e 's/  $enable_memtrace_cmd $numactl/  nice $enable_memtrace_cmd $numactl/g' -i /opt/adaptv/new_adserver/adserver;sudo -Hu adaptv /opt/adaptv/new_adserver/adserver start", stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex3 = p3.wait()
        path = os.path.realpath('/opt/adaptv/new_adserver')
        if revid == path.split('.')[1]: 
            print 'adserver deployed'
            exit_deploy()
        elif current_revid_adserver == path.split('.')[1]:
            if retry <1:
                retry = retry + 1
                start()
    except:
        pass

def deploy_shortribs(revid):
    try: 
        global current_revid_adserver,retry
        p1 = subprocess.Popen('sudo -Hu adaptv /opt/adaptv/shortribs/shortribs stop', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex1 = p1.wait()
        p5 = subprocess.Popen('sudo -Hu adaptv s3cmd --skip-existing get s3://atv.adserver.autoscaling.'+region+'/shortribs/shortribs*14.04.tgz /opt/adaptv/', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True)
        e5 = p5.wait()
        p8 = subprocess.Popen('sudo -Hu adaptv s3cmd --skip-existing get s3://atv.adserver.autoscaling.'+region+'/shortribs/'+region+zone+'/hosts.conf /opt/adaptv/etc/shortribs/', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex8 = p8.wait()
        p9 = subprocess.Popen('sudo -Hu adaptv s3cmd --skip-existing get s3://atv.adserver.autoscaling.'+region+'/shortribs/'+region+zone+'/extra_args.conf /opt/adaptv/etc/shortribs/', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex9 = p9.wait()
        p2 = subprocess.Popen('sudo -Hu adaptv tar -zxf /opt/adaptv/shortribs.'+revid+'*.tgz -C /opt/adaptv/', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex2 = p2.wait()
        p3 = subprocess.Popen('sudo -Hu adaptv /opt/adaptv/shortribs/shortribs start', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex3 = p3.wait()
        path = os.path.realpath('/opt/adaptv/shortribs')
        if revid == path.split('.')[1]:
            print 'shortribs deployed'
            exit_deploy()
        elif current_revid_shortribs == path.split('.')[1]:
            if retry <1:
                retry = retry + 1
                start()
    except:
        pass

def deploy_geoip(revid):
    try:
        global current_revid_adserver,retry
        p5 = subprocess.Popen('sudo -Hu adaptv s3cmd --skip-existing get s3://atv.adserver.autoscaling.'+region+'/geoip/geoip*14.04.tgz /opt/adaptv/', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True)
        e5 = p5.wait()
        p2 = subprocess.Popen('sudo -Hu adaptv tar -zxf /opt/adaptv/geoip.'+revid+'*.tgz -C /opt/adaptv/', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex2 = p2.wait()
        p3 = subprocess.Popen('sudo -Hu adaptv /opt/adaptv/geoip/geoip deploy', stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
        ex3 = p3.wait()
        path = os.path.realpath('/opt/adaptv/geoip')
        if revid == path.split('.')[1]:
            print 'geoip deployed'
            exit_deploy()
        elif current_revid_adserver == path.split('.')[1]:
            if retry <1:
                retry = retry + 1
                start()
    except:
        pass
 


def exit_deploy():
    try:
        client.exit_standby(
            InstanceIds=[
                instanceid,
            ],
            AutoScalingGroupName='adserver-'+zone+'-'+instancetype,
    )
    except Exception,e:
        print e
        print 'Not in standby'    
        pass

def start():
    check_deploy_adserver()
    check_deploy_shortribs()
    check_deploy_geoip()
    exit_deploy()
    os.remove('/var/run/autoscale_monitor.lock')
     
if __name__ == "__main__":
    start()
