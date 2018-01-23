#!/usr/bin/python

import boto3
import botocore
import ConfigParser
import datetime
import dns.zone
import logging
import optparse
import os
import smtplib
import socket
import sys
import logging
from logging import handlers
from boto.route53.connection import Route53Connection
from boto.route53.exception import DNSServerError

## General
dnsRDATATypes = ['A','CNAME','AAAA','MX','SPF','PTR']
awsUPDATEList = []
usageString = """Usage: %prog [options]"""
configFile = 'sync-bind-to-aws.conf'
hostName = socket.gethostname()
alertMessage = """From: AWS DNS Admin <noreply@linuz.in>
To: %(tolist)s
Subject: %(subj)s

%(body)s
"""

## Parse the options for script
def create_option_parser():
    parser = optparse.OptionParser(usage=usageString)

    parser.add_option('-f', '--file',
                     type='string',
                     action='store',
                     dest='conf_file',
                     help='Configuration file')

    return parser

## Read configuration
def read_config():
    """
    Read & initialize the configuration settings.
    """
    option_parser = create_option_parser()
    options, arguments = option_parser.parse_args()
    config = ConfigParser.RawConfigParser()

    config_file = options.conf_file

    if not config_file:
        config_file = configFile

    if not os.path.isfile(config_file):
        logging.error('Configuration file %s not found!!' % (config_file,))
        sys.exit(1)
    else:
        config.read(config_file)

    options.alertSender = config.get('Alert','alert_sender')
    options.alertReceivers = config.get('Alert','alert_receivers')
    options.bindZONEFile = config.get('Bind', 'bind_zone_file')
    options.awsHOSTEDZoneID = config.get('Route53', 'aws_hosted_zoneid')
    options.awsRCDttl = config.get('Route53', 'aws_record_ttl')
    options.logFILEName = config.get('Logging', 'log_file_name')
    options.logLevel = config.get('Logging', 'log_level')
    options.logFormat = config.get('Logging', 'log_format')
    options.backupLogs = config.get('Logging', 'backup_logs')
    options.maxSize = config.get('Logging', 'max_size')

    return options

## Configure log handler
def setup_logging(log_file=None, log_level=logging.INFO, \
              log_format=None, max_size=10000000, backup_logs=10):

    if log_format is None:
        log_format = '%(asctime)s %(module)s %(levelname)-8s- %(message)s'
    logger = logging.getLogger()
    logger.setLevel(log_level)

    file_handler = handlers.RotatingFileHandler(log_file,
            maxBytes=int(max_size),
            backupCount=int(backup_logs))

    file_handler.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

## Class which identifies difference between bind zone data and
## Route53 zone data
class dictDiffer(object):
    def __init__(self, dictOne, dictTwo):
        self.dictOne, self.dictTwo = dictOne, dictTwo
        self.setDictone, self.setDicttwo = set(dictOne.keys()), set(dictTwo.keys())
        self.intersect = self.setDictone.intersection(self.setDicttwo)
    def added(self):
        return self.setDictone - self.intersect
    def removed(self):
        return self.setDicttwo - self.intersect
    def changed(self):
        return set(o for o in self.intersect if self.dictTwo[o] != self.dictOne[o])

## To notify if any issues during script execution
def notify(alertSubject,alertBody):
    try:
        alertReceivers = ','.join('<{0}>'.format(w) for w in config_data.alertReceivers.split(",")).split(",")
        logging.error("%s \n %s"%(alertSubject,alertBody))
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(config_data.alertSender, alertReceivers, alertMessage %
                {"tolist": ','.join(alertReceivers), "subj": alertSubject, "body": alertBody})
    except ( smtplib.SMTPException, socket.error):
        logging.error("Error: unable to send email")

## Create dict which contain bind zone data
def getBINDZonedata(bindZONEFile):
    dnsLOCALDict = {}

    if not os.path.isfile(bindZONEFile):
        logging.error('Bind zone file %s not found!!' % (bindZONEFile))
        sys.exit(1)

    try:
        z = dns.zone.from_file(bindZONEFile,relativize=False,allow_include=True)
    except dns.exception.DNSException as e:
        notify("Incorrect zone data. No records updated or deleted",e)
        sys.exit(1)
    for rcdType in dnsRDATATypes:
        b = z.iterate_rdatas(rcdType)
        d = dnsLOCALDict.setdefault(rcdType, {})
        for a in b:
            e = d.setdefault(str(a[0]), [])
            if d.has_key(str(a[0])):
                d[str(a[0])].append(str(a[2]))
            else:
                d[str(a[0])] = str(a[2])
    return dnsLOCALDict

## Create dict which contain Route53 data
def getAWSRoute53data(awsHOSTEDZoneID):
    dnsREMOTEDict = {}

    try:
        route53 = Route53Connection()
        sets = route53.get_all_rrsets(awsHOSTEDZoneID)
    except ( socket.error, DNSServerError ) as e:
        notify("Unable to get resource record data from Route53",e)
        sys.exit(1)
    for rset in sets:
        d = dnsREMOTEDict.setdefault(str(rset.type), {})
        for rcd in rset.resource_records:
            e = d.setdefault(str(rset.name), [])
            d[str(rset.name)].append(str(rcd))
    return dnsREMOTEDict

## List contain resource records which needs to update
def updateAWSRecordlist(action,rcdName,rcdValue,rcdType):
    updateValue = []
    for a in rcdValue:
        updateValue.append({ 'Value': a})

    awsUPDATEList.append({
        'Action': action,
        'ResourceRecordSet': { 'Name': rcdName, 'Type': rcdType,
                                        'TTL': int(config_data.awsRCDttl),
        'ResourceRecords': updateValue,}
        },)

def main():

    global config_data

    config_data = read_config()

    setup_logging(config_data.logFILEName, config_data.logLevel,
        config_data.logFormat, config_data.maxSize, config_data.backupLogs)

    localZONEData = getBINDZonedata(config_data.bindZONEFile)
    remoteZONEData = getAWSRoute53data(config_data.awsHOSTEDZoneID)

    add = delt = upt = 0

    added_dt = []
    deleted_dt = []
    updated_dt = []

    for rcdType in dnsRDATATypes:

        ## If resource record is not present on Route53 it should be added

        if remoteZONEData.has_key(rcdType):
            f = dictDiffer(localZONEData[rcdType],remoteZONEData[rcdType])

            if f.added():
                for rcdName in f.added():
                    rcdValue = localZONEData[rcdType][rcdName]
                    add += 1
                    added_dt.append("%s[%s]%s"%(rcdName,rcdType,rcdValue))
		    updateAWSRecordlist('CREATE',rcdName,rcdValue,rcdType)
            if f.removed():
                for rcdName in f.removed():
                    rcdValue = remoteZONEData[rcdType][rcdName]
                    delt += 1
                    deleted_dt.append("%s[%s]%s"%(rcdName,rcdType,rcdValue))
                    updateAWSRecordlist('DELETE',rcdName,rcdValue,rcdType)
            if f.changed():
                for rcdName in f.changed():
                    rcdValue = localZONEData[rcdType][rcdName]
                    upt += 1
                    updated_dt.append("%s[%s]%s"%(rcdName,rcdType,rcdValue))
                    updateAWSRecordlist('UPSERT',rcdName,rcdValue,rcdType)
        else:
            for rcdName in localZONEData[rcdType]:
                rcdValue = localZONEData[rcdType][rcdName]
                add += 1
                added_dt.append("%s[%s]%s"%(rcdName,rcdType,rcdValue))
                updateAWSRecordlist('CREATE',rcdName,rcdValue,rcdType)

    if awsUPDATEList:

        ## A request cannot contain more than 1000 ResourceRecord elements. When the value of the
        ## Action element is UPSERT, each ResourceRecord element is counted twice. We should generate
        ## an alert if single execution have more than 500 elements to update because its not normal
        ## over a period of 30 mins

        if len(awsUPDATEList) > 500:
            e = "Please verify DNS change list \n %s"%awsUPDATEList
            notify("Update DNS change list size is large ",e)
            sys.exit(1)

        try:
            client = boto3.client('route53')
            timeStamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updateComment = "DNS update at %s from %s"%(timeStamp, hostName)
            response = client.change_resource_record_sets(
                HostedZoneId = config_data.awsHOSTEDZoneID,
                    ChangeBatch={
                        'Comment': updateComment,
                        'Changes': awsUPDATEList
                    })
        except ( botocore.exceptions.ClientError, socket.error) as e:
            notify("Unable to update Route53",e)
            sys.exit(1)

    logging.info("Total Records Added: %s, Deleted: %s, Updated: %s"%(add,delt,upt))
    if added_dt:
        logging.info("Records Added: %s"%added_dt)
    if deleted_dt:
        logging.info("Records Deleted: %s"%deleted_dt)
    if updated_dt:
        logging.info("Records Updated: %s"%updated_dt)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        e = 'Execution interrupted manually'
        notify('Script interrupted manually, exiting.', e)
    except Exception as e:
        notify('Script execution interrupted! Error: %s', e)
