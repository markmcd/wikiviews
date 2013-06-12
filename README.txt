ABOUT

This app provides a modular input to Splunk that indexes some (or all) of the
various Wikipedia site's page view data.


CONFIGURATION

The directions on the modular input page should be fairly clear, but you want to
ensure you set a checkpoint to start from, a project include regex and a page
exclude regex.

The checkpoint defines where the script will begin importing data from.  The
Wikipedia pageview data is quite large, so you probably want to pick today's
date.  The format is YYYYMMDDHH, to start from midnight (UTC) on June 30, 2013
you would use '2013063000'.

The project include regex defines which Wikipedia projects you want to include
in your input.  Each pageview record contains a field that will look like one
of these:

 fr.b
 en
 en.mw
 ...

The characters before the period are the language characters and the [optional]
extension is the Wikipedia project to include.  When omitted it is the core
Wikipedia project and when provided it is one of these:

 wikibooks: ".b"
 wiktionary: ".d"
 wikimedia: ".m"
 wikipedia mobile: ".mw"
 wikinews: ".n"
 wikiquote: ".q"
 wikisource: ".s"
 wikiversity: ".v"
 mediawiki: ".w"

Be sure to include your start and end regex anchors, like '^en$'.

The page exclude regex lets you choose which, if any, pages you want excluded
from the index.  The pageview records include the Wikipedia 'Special:Whatever'
pages, as well as discussion pages.  If you want to include everything, just
use '^$'.


DATA NOTES AND WARNING

The raw data is provided by the Wikimedia foundation from here:
  http://dumps.wikimedia.org/other/pagecounts-raw/

Data are provided in an aggregated hourly fashion and this script will pull
down hourly blocks one-by-one and ingest them.  In order to work with Splunk's
maximum of 1,000 events per second, this script randomises the hour & minute
that the event occurred, hopefully distributing the events evenly.

Because of this, I *strongly* recommend you only import data into a dedicated
index.  This will make it easy to blow away, but also reduce the chance that
you will hit the 1k events/sec/index limit.

This is a fairly large dataset.  Do not use on a production Splunk instance
unless you are willing to destroy your license.  A once-off ingest should be
fine, but bare in mind that it will take days to import even a small range of
dates.  When building the app, the data sizes looked like this, when using
only the English Wikipedia page:

 * Downloading data consumes about 100MB for every hour you download
 * Ingesting ALL sites would consume about 400MB for every hour you ingest
 * Filtering to just the English Wikipedia reduces that to 100MB per hour
 * en.wiki produces about 40,000 events per second in it's index
 * en.wiki will consume 4GB/day volume on it's own - YOU HAVE BEEN WARNED

