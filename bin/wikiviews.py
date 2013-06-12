#!/usr/bin/python
# vim: ai si sw=4 ts=4 et

# ~100MB / hr download (gz) ~2.4GB/day
# ~400MB / hr ingest (raw)  ~4GB/day
# ~100MB / hr for just 'en wikipedia'


import datetime
import requests
import StringIO
import gzip
import re
import sys, os
import logging
import splunk.entity as entity
import xml
import random

# splunk friendly logging
logging.root
logging.root.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.root.addHandler(handler)

fmt = "%Y%m%d%H" # YYYYMMDDHH

def pull_data(test_chkpt = None):
    # load values
    if test_chkpt:
        checkpoint = test_chkpt
        projectinclude = '^en$'
        pageexclude = '^$'
    else:
        config = get_config()
        checkpoint = get_checkpoint_from_file(config, config["checkpoint"])
        projectinclude = config["projectinclude"]
        pageexclude = config["pageexclude"]

    # set up the date imports
    chkdate = datetime.datetime.strptime(checkpoint, fmt)
    targetchkdate = datetime.datetime.utcnow()
    targetcheckpoint = targetchkdate.strftime(fmt)

    # set up the string regexes
    projectmatch = re.compile(projectinclude)
    pagenomatch = re.compile(pageexclude)

    # do the work
    iterdate = chkdate + datetime.timedelta(hours=1)
    while (iterdate <= targetchkdate):
        logging.info("action='processcheckpoint' checkpoint='%s'" % iterdate.strftime(fmt))

        # download the file, attempting to catch the second variable correctly
        # (almost all files are at t=HH:00:00, but some are a few seconds past)
        i = 0
        success = False
        while not success and i < 60:
            url = iterdate.strftime("http://dumps.wikimedia.org/other/pagecounts-raw/%Y/%Y-%m/pagecounts-%Y%m%d-%H000"+str(i)+".gz")
            r = requests.get(url)
            logging.info("action='downloadeddata' checkpoint='%s' filesize='%d' i='%d' code='%d'" % (iterdate.strftime(fmt), len(r.content), i, r.status_code))
            if (r.status_code == 404):
                i += 1
                logging.warn("Data file not where it should be, incrementing second counter to %d and trying again."%i)
            elif (r.status_code == 200):
                success = True
                # handle everything in memory like a cowboy
                gzfile = StringIO.StringIO(r.content)
                datafile = gzip.GzipFile(fileobj=gzfile, mode='rb')
                for line in datafile:
                    chunks = line.split()
                    if projectmatch.match(chunks[0]) and not pagenomatch.match(chunks[1]):
                        # since we likely have >1000 events we want to spread them out 
                        # over the whole hour, otherwise splunk gets sad
                        timesuffix = "%02d:%02d +0000 " % (random.randint(0, 59), random.randint(0, 59))
                        print iterdate.strftime("%a, %d %b %Y %H:"+timesuffix) + line.rstrip()
                if not test_chkpt:
                    write_checkpoint_to_file(config, iterdate.strftime(fmt))

        if not success:
            logging.error("unable to retrieve checkpoint %s" % iterdate.strftime(fmt))
        
        iterdate = iterdate + datetime.timedelta(hours=1)

# TODO: generate 'checkpoint' filename from input parameters
def get_checkpoint_from_file(config, default):
    fname = os.path.join(config["checkpoint_dir"], "checkpoint")
    if not os.path.exists(fname):
        with open(fname, "w+") as f:
            f.write(default)

    chk_file = open(os.path.join(config["checkpoint_dir"], "checkpoint"), "r+")
    try:
        chkpt = chk_file.read()
    except:
        chkpt = default
    return chkpt


def write_checkpoint_to_file(config, chkpt):
    chk_file = open(os.path.join(config["checkpoint_dir"], "checkpoint"), "w+")
    chk_file.write(chkpt)


SCHEME = """<scheme>
    <title>WikiViews</title>
    <description>Load pageview data from Wikipedia sites</description>
    <use_external_validation>false</use_external_validation>
    <streaming_mode>simple</streaming_mode>
    <use_single_instance>false</use_single_instance>
    <endpoint>
        <args>
            <arg name="checkpoint">
                <title>Earliest import date</title>
                <description>YYYYMMDDHH that will be used as the 'last successful' import.  e.g. To start from Jan 1, 2013 use 2012123123</description>
                <validation>validate(match('checkpoint', '^\d{10}$'), "Earliest import date is not in valid format")</validation>
                <required_on_create>true</required_on_create>
                <required_on_edit>false</required_on_edit>
            </arg>

            <arg name="projectinclude">
                <title>Wikipedia Projects to Include</title>
                <description>A regular expression that will include all matching projects &amp; languages.  For just the English Wikipedia, use '^en$', for all English projects use '^en'.  For just the French Wikibooks, use '^fr.b$'.  Format is 'language.project', where language is the 2-char language code and project is one of: wikibooks '.b', wiktionary '.d', wikimedia '.m', wikipedia mobile '.mw', wikinews '.n', wikiquote '.q', wikisource '.s', wikiversity '.v', mediawiki '.w' and projects with no period &amp; following character are wikipedia.</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>true</required_on_create>
            </arg>

            <arg name="pageexclude">
                <title>Pages to Exclude</title>
                <description>Use this regular expression to exclude any pages from being indexed.  To include everything use '^$', to exclude the "Special:" pages, use '^Special(:|%3a|%3A|/)'.</description>
                <required_on_create>true</required_on_create>
                <required_on_edit>false</required_on_edit>
            </arg>
        </args>
    </endpoint>
</scheme>
"""

def do_scheme():
    print SCHEME

def validate_conf(config, key):
    if key not in config:
        raise Exception, "Invalid configuration received from Splunk: key '%s' is missing." % key

def get_config():
    config = {}

    try:
        # read everything from stdin
        config_str = sys.stdin.read()

        # parse the config XML
        doc = xml.dom.minidom.parseString(config_str)
        root = doc.documentElement
        conf_node = root.getElementsByTagName("configuration")[0]
        if conf_node:
            logging.debug("XML: found configuration")
            stanza = conf_node.getElementsByTagName("stanza")[0]
            if stanza:
                stanza_name = stanza.getAttribute("name")
                if stanza_name:
                    logging.debug("XML: found stanza " + stanza_name)
                    config["name"] = stanza_name

                    params = stanza.getElementsByTagName("param")
                    for param in params:
                        param_name = param.getAttribute("name")
                        logging.debug("XML: found param '%s'" % param_name)
                        if param_name and param.firstChild and \
                           param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                            data = param.firstChild.data
                            config[param_name] = data
                            logging.debug("XML: '%s' -> '%s'" % (param_name, data))

        checkpnt_node = root.getElementsByTagName("checkpoint_dir")[0]
        if checkpnt_node and checkpnt_node.firstChild and \
           checkpnt_node.firstChild.nodeType == checkpnt_node.firstChild.TEXT_NODE:
            config["checkpoint_dir"] = checkpnt_node.firstChild.data

        if not config:
            raise Exception, "Invalid configuration received from Splunk."

        # just some validation: make sure these keys are present (required)
        validate_conf(config, "checkpoint")
        validate_conf(config, "projectinclude")
        validate_conf(config, "pageexclude")
        validate_conf(config, "checkpoint_dir")
    except Exception, e:
        raise Exception, "Error getting Splunk configuration via STDIN: %s" % str(e)

    return config

# inputs:

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":
            do_scheme()
        elif sys.argv[1] == "--test":
            # run the import for the last published file
            pull_data((datetime.datetime.utcnow() - datetime.timedelta(hours=1)).strftime(fmt))
        else:
            print 'You giveth weird arguments'
    else:
        pull_data()

    sys.exit(0)


