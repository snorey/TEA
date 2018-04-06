import datetime
import os
import re
import time
import urllib
import urllib2

import idem_settings

DEFAULT_WAIT = 3
DEFAULT_WAIT_AFTER_ERROR = 30
TIMEOUT = 100
RETRY_LIMIT = 10
NUM_COORD_DIGITS = 3

# core functionality for TEA project
# certain concepts are consistent across applications:


class Facility(object):  # data structure
    name = ""
    city = ""
    street_address = ""
    county = ""
    state = ""
    zip = ""
    latlong = ()
    full_address = ""
    vfc_id = ""
    vfc_url = ""
    vfc_name = ""
    vfc_address = ""
    docs = []
    updated_docs = set()
    downloaded_docs = set()
    directory = ""
    row = ""
    page = ""
    date = datetime.date.today()
    parent = ""

    def __init__(self, **arguments):
        assign_values(self, arguments)
        if "directory" in arguments.keys():
            self.set_directory(directory=arguments["directory"])
        else:
            self.set_directory()
        if "row" in arguments.keys():
            self.from_row()
        if "parent" in arguments.keys():
            self.zip = parent.zip
        self.docs = self.get_docs()
        self.get_latest_page()
#        if not self.page:
#            print "retrieving page"
#            self.page = self.retrieve_page()
#            print len(self.page)
        if not self.downloaded_docs:
            self.downloaded_docs = self.get_downloaded_docs()
        self.docs = self.docs_from_page(self.page)

    def __eq__(self, other):
        if hasattr(other, "identity"):
            return self.identity == other.identity
        else:
            return False

    def __hash__(self):
        return hash(self.identity)

    @property
    def identity(self):
        return self.name, self.city, self.latlong

    @property
    def latitude(self):
        return float(self.latlong[0])

    @property
    def longitude(self):
        return float(self.latlong[1])

    def set_directory(self, directory=False):
        if directory:
            self.directory = directory
        elif hasattr(self, "parent"):
            self.directory = os.path.join(self.parent.directory, self.vfc_id)
        else:
            self.directory = self.vfc_id
        if not os.path.isdir(self.directory):
            os.mkdir(self.directory)
        return self.directory

    def get_docs(self):
        return set(os.listdir(self.directory))

    def get_downloaded_docs(self):
        return set(os.listdir(self.directory))

    @staticmethod
    def get_latest_page():
        pass

    @staticmethod
    def retrieve_page():
        return ""

    @staticmethod
    def docs_from_page(page=""):
        if not hasattr(self, page) or not page:
            self.page = page
        return set()


class Document(object):
    url = ""
    crawl_date = datetime.date.today()
    file_date = False
    id = ""
    doc_type = ""
    program = ""
    filename = ""
    facility = ""
    path = ""
    content = ""

    def __init__(self, **arguments):
        assign_values(self, arguments)

    def __eq__(self, other):
        if type(other) == str:
            return self.filename == other
        else:
            return self.filename == other.filename

    def __hash__(self):
        return hash(self.filename)

    @property
    def date(self):
        if self.file_date:
            return self.file_date
        else:
            return self.crawl_date

    def retrieve_patiently(self, path=""):
        if not path:
            path = self.path
        done = False
        inc = 0
        while not done:
            inc += 1
            if inc > RETRY_LIMIT:
                print "Aborting!"
                return False
            try:
                urllib.urlretrieve(self.url, path)
            except Exception, e:
                print str(e)
                time.sleep(DEFAULT_WAIT_AFTER_ERROR)
            else:
                done = True
                time.sleep(DEFAULT_WAIT)

    def to_tsv(self):
        data = [self.crawl_date, self.filename, self.url]
        tsv = "\t".join(data)
        return tsv


def assign_values(obj, arguments):
    for key, value in arguments.items():
        #        if not isinstance(value, basestring):
        #            print "Ignoring value %s for %s" % (str(value), str(key))
        #            continue
        setattr(obj, key, value)


def get_previous_file_in_directory(directory,
                                   pattern=".*(\d{4}-\d{2}-\d{2})",
                                   reference_date=datetime.date.today().isoformat()):
    # note that this function requires that the directory have only one file matching the pattern per date
    def is_dated_file(file_in_directory):
        return re.match(pattern, file_in_directory)
    if not isinstance(reference_date, basestring):
        reference_date = reference_date.isoformat()
    files = os.listdir(directory)
    files = [x for x in files if is_dated_file(x)]
    dated_files = []
    for filename in files:
        date = re.search(pattern, filename).group(1)
        dated_files.append((date, filename))
    dated_files.sort()
    dated_files.reverse()
    for date, filename in dated_files:  # starting from most recent
        if date < reference_date:
            filepath = os.path.join(directory, filename)
            return filepath


def coord_from_address(address):
    apikey = idem_settings.google_maps_key
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s"
    url = url % (urllib.quote(address), apikey)
    apipage = urllib2.urlopen(url).read()
    try:
        geometry = apipage.split('"geometry"')[1].split('"location"')[1]
    except IndexError:
        return False
    latitude = geometry.split('"lat"')[1].split(':')[1].split(",")[0].split("\n")[0].strip()
    latitude = str(round(float(latitude), NUM_COORD_DIGITS))
    longitude = geometry.split('"lng"')[1].split(':')[1].split(",")[0].split("\n")[0].strip()
    longitude = str(round(float(longitude), NUM_COORD_DIGITS))
    googleadd = apipage.split('"formatted_address"')[1].split('"')[1].split('"')[0].strip()
    return latitude, longitude, googleadd


def do_patiently(action,*args,**kwargs):
    done = False
    inc = 0
    while not done:
        inc += 1
        if inc > RETRY_LIMIT:
            print "Aborting!"
            return False
        try:
            result = action(*args,**kwargs)
        except Exception, e:
            print str(e)
            time.sleep(DEFAULT_WAIT_AFTER_ERROR)
        else:
            done = True
            time.sleep(DEFAULT_WAIT)
    return result


def retrieve_patiently(url, path):
    do_patiently(urllib.urlretrieve,url,path)
    return path


