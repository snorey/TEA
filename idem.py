# -*- coding: utf-8 -*-

# de-spaghettify
# unit testing
# objects for enforcements
# objects for collections (zips,docs,facs)
# effective cross-referencing
# instant reanimations
# structure for storing facility data independently of source
# inserting links for CAA & NPDES permits
# include list of open permits in daily update - DONE
# fix recurrent log glitches

import collections
import datetime
import os
import re
import time
import unittest
import urllib
import urllib2
import xml.parsers.expat

import idem_settings

lakezips = idem_settings.do_lake_zips
downloadzips = idem_settings.downloadzips
maindir = idem_settings.maindir
permitdir = idem_settings.permitdir
enforcementdir = idem_settings.enforcementdir
innddir = idem_settings.innddir


class TotalUpdater:
    def __init__(self):
        pass

    def go(self):
        pass
        # cases
        # enforcements
        # permits
        # zips

### zips

def do_lake_zips():
    batch = ZipCollection(lakezips, firsttime=False)
    batch.go(finish=True)
    return batch


class ZipCollection(list):
    html = ""
    date = datetime.date.today()
    facilities = set()
    iddic = {}
    namedic = collections.defaultdict(list)

    def __init__(self, zips=lakezips, offline=False, **kwargs):
        self.zips = zips
        self.offline = offline
        for zip in zips:
            updater = ZipUpdater(zip)
            self.append(updater)
            for facility in updater.facilities:
                self.facilities.add(facility)
                self.iddic[facility.vfc_id] = facility
                self.namedic[facility.vfc_name].append(facility)
            if offline:
                updater.go_offline()
            else:
                updater.whether_update_zip_info = (self.date.day % 7 == int(zip) % 7)  # to prevent all from bunching up
                updater.whether_download = zip in downloadzips
            for key, value in kwargs.items():
                setattr(updater, key, value)

    def go(self, finish=True):
        for updater in self:
            print "***%s***" % str(updater.zip)
            updater.do_zip()
        if finish is True:
            self.to_html()
            htmlname = "test_%s.html" % self.date.isoformat()
            htmlpath = os.path.join(maindir, htmlname)
            open(htmlpath, "w").write(self.html)
            import webbrowser
            webbrowser.open(htmlpath)

    def to_html(self):
        htmlpattern = '<h3>New VFC records</h3>\n<table><tr style="background-color:#eeeeff"><th colspan="3">New files found on %s</th></tr>\n'
        html = htmlpattern % self.date.strftime("%A %B %d, %Y")
        html += '<tr style="background-color:#eeeeff"><th width="30%">Site</th><th width="30%">Date</th><th width="40%">Document</th></tr>\n'
        for updater in self:
            html += updater.html
        html += "</table>"
        self.html = html
        return html

    def find_by_name(self, searchterm):
        searchterm = searchterm.upper()
        matches = filter(lambda x: searchterm in x, self.namedic.keys())
        found = []
        for m in matches:
            found.extend(self.namedic[m])
        return found

    def get_facilities_within(self, point, maxdistance):
        facilities_in_range = []
        for facility in self.facilities:
            if not hasattr(facility, "latlong"):
                continue
            elif facility.latlong is False:
                continue
            else:
                distance = get_distance(point, facility.latlong)
                if distance <= maxdistance:
                    facilities_in_range.append((distance, facility))
        facilities_in_range.sort()
        facilities = [x[1] for x in facilities_in_range]
        return facilities

    def reload_latlongs(self, filepath="facilitydump.txt", idcol=0, llcol=2, addcol=3):
        for line in open(filepath):
            if not line.strip() or "\t" not in line:
                continue
            pieces = line.split("\t")
            id = pieces[idcol]
            if addcol is not False:
                add = pieces[addcol]
            latlongstring = pieces[llcol]
            #			"(41.62111, -87.46647)"
            if ", " not in latlongstring:
                continue
            latstring, longstring = latlongstring.split(", ")
            lat = float(latstring[1:])
            lon = float(longstring[:-1])
            facility = self.iddic[id]
            facility.latlong = (lat, lon)
            facility.address = add


class ZipUpdater:
    def __init__(self, zip):
        self.zip = zip
        self.html = ""
        self.whether_download = True
        self.whether_update_zip_info = True
        self.whether_update_facility_info = True
        self.offline = False
        self.firsttime = True
        self.directory = os.path.join(maindir, zip)
        self.date = datetime.date.today()
        self.zipurl = build_zip_url(zip)
        try:
            os.mkdir(self.directory)
        except Exception, e:
            pass
        self.page = get_latest_zip_page(self.zip, zipdir=self.directory)
        self.facilities = self.get_facilities_from_page(self.page)
        self.siteids = dict(map(lambda x: (x.vfc_id, x), self.facilities))

    def go_offline(self):
        self.whether_download = False
        self.whether_update_zip_info = False
        self.whether_update_facility_info = False
        self.offline = True

    def do_zip(self, startfrom=False):
        zip = self.zip
        firsttime = self.firsttime
        self.html = ""  # reset
        if self.whether_update_zip_info is True:
            self.retrieve_zip_page()
            self.facilities = self.get_facilities_from_page(self.page)
            self.siteids = dict(map(lambda x: (x.vfc_id, x), self.facilities))
        if self.siteids:
            self.get_updated_facilities()
            self.html += '<tr style="background-color:#eeeeff"><td colspan = "3">%s</td></tr>' % self.zip
            for facility in self.updated_facilities:
                self.html += self.build_facility_rows(facility)
            self.log_updates_ECM()
        else:
            print "No IDs found!"

    def build_facility_rows(self, facility):
        html = ""
        docs = sorted(list(facility.updated_docs))
        rowspan = len(docs)
        sitelink = '<a href="%s">%s</a>' % (facility.vfc_url, facility.vfc_name)
        sitecell = '<td rowspan="%d">%s</td>\n' % (rowspan, sitelink)
        for doc in docs:
            datestring = doc.file_date.strftime("%B %d, %Y")
            datecell = "<td>%s</td>\n" % datestring
            filelink = '<a href="%s">%s</a>' % (doc.url, doc.id)
            filetext = "%s (%s - %s)" % (filelink, doc.program, doc.type)
            filecell = "<td>%s</td>\n" % filetext
            index = docs.index(doc)
            if index == 0:
                row = '<tr>%s\n%s\n%s</tr>\n' % (sitecell, datecell, filecell)
            else:
                row = '<tr>%s\n%s</tr>\n' % (datecell, filecell)
            html += row
        return html

    def retrieve_zip_page(self):
        try:
            zippage = urllib2.urlopen(self.zipurl, timeout=100).read()
        except urllib2.URLError, e:
            return False
        matchme = "Displaying Facilities 1 - (\d+) of (\d+)"
        matched = re.search(matchme, zippage)
        if matched:
            thispage, totalcount = matched.group(1), matched.group(2)
            print self.zip, thispage, totalcount
            if int(thispage) < int(totalcount):
                print "fetching page 2..."  # nothing currently gets close to page 3
                nexturl = zipurl + "&PageNumber=2"
                nextpage = urllib2.urlopen(nexturl, timeout=100).read()
                zippage += nextpage
        zippagepath = os.path.join(self.directory, str(self.zip) + "_" + self.date.isoformat() + ".html")
        open(zippagepath, "w").write(zippage)
        self.page = zippage
        return zippage

    def get_updated_facilities(self):
        siteids = sorted(self.siteids.keys())
        self.updated_facilities = []
        for id in siteids:
            self.facility = self.siteids[id]
            sincecheck = since_last_scan(self.facility.directory)
            sincenewfile = since_last_file(self.facility.directory, download=self.whether_download)
            if self.firsttime is False and self.whether_update_facility_info is True:  # if not updating, no need to be skimpy
                if sincecheck < 1:
                    continue
                elif sincenewfile > 30 and sincecheck < 3:  # somewhat arbitrary numbers here
                    continue
                elif sincenewfile > 350 and sincecheck < 10:
                    continue
                elif sincenewfile > 1500 and sincecheck < 50:
                    continue
            print self.facility.vfc_name, self.facility.vfc_id, "%d/%d" % (siteids.index(id) + 1, len(siteids))
            self.fetch_facility_docs()
            if self.facility.updated_docs:
                print len(self.facility.updated_docs)
                self.updated_facilities.append(self.facility)
            else:
                continue
            if self.whether_download is True:
                time.sleep(5)
        return self.updated_facilities

    def log_updates_ECM(self):
        directory = self.directory
        date = self.date
        filename = "updates_%s_%s.txt" % (self.zip, date.isoformat())
        filepath = os.path.join(directory, filename)
        self.logtext = self.build_facility_log()
        if self.logtext:
            writefile = open(filepath, "a")
            with writefile:
                writefile.write(self.logtext)

    def build_facility_log(self):
        text = ""
        for facility in self.updated_facilities:
            sitename = facility.vfc_name
            for newfile in facility.updated_docs:
                newline = "%s\t%s\t%s\t%s\t%s\n" % (
                newfile.file_date.isoformat(), facility.vfc_id, facility.vfc_name, newfile.filename, newfile.url)
                text += newline
        return text

    def reconstruct_site_list(self):
        self.page = get_latest_zip_page(self.zip, zipdir=self.directory)
        sites_from_page = self.get_facilities_from_page(self.page)
        siteids = dict(map(lambda x: (x.vfc_id, x), sites_from_page))
        self.siteids = siteids
        return siteids

    def get_downloaded_docs(self):
        directory = self.check_and_make_facility_directory()
        already = os.listdir(directory)
        already = set(filter(lambda x: x.endswith(".pdf"), already))
        self.facility.downloaded_docs = already

    def fetch_facility_docs(self):
        self.get_downloaded_docs()
        self.facility.updated_docs = self.facility.get_updated_docs_in_directory()
        if self.facility.updated_docs:
            print self.facility.vfc_name, len(self.facility.updated_docs)
        if self.whether_update_facility_info is True:
            page = self.retrieve_facility_page()
        else:
            page = self.facility.get_latest_page()
        self.facility.page = page
        self.facility.updated_docs |= self.fetch_files_for_facility()
        return self.facility.updated_docs

    def retrieve_facility_page(self):
        if self.firsttime is False and len(self.facility.downloaded_docs) > 0:
            resultcount = 20
        else:
            resultcount = 500
        starturl = "https://ecm.idem.in.gov/cs/idcplg?IdcService=GET_SEARCH_RESULTS"
        starturl += "&QueryText=xAIID+%3Ccontains%3E+`" + self.facility.vfc_id + "`"
        starturl += "&listTemplateId=SearchResultsIDEM&searchFormType=standard"
        starturl += "&SearchQueryFormat=UNIVERSAL&ftx=&AdvSearch=True&ResultCount="
        starturl += str(resultcount) + "&SortField=dInDate&SortOrder=Desc"
        self.facility.page = urllib2.urlopen(starturl, timeout=100).read()
        pagefilename = self.facility.vfc_id + "_" + self.date.isoformat()
        pagepath = os.path.join(self.facility.directory, pagefilename)
        open(pagepath, "w").write(self.facility.page)
        return self.facility.page

    def check_and_make_facility_directory(self):
        self.facility.directory = os.path.join(self.directory, self.facility.vfc_id)
        try:
            os.mkdir(self.facility.directory)
        except Exception, e:
            pass
        else:
            self.firsttime = True  # if directory not created, we know it hasn't been checked before
        return self.facility.directory

    def get_updated_files_without_downloading(self):
        directory = self.directory
        delta = datetime.timedelta(1)
        yesterday = datetime.date.today() - delta
        files = os.listdir(directory)
        previous = filter(lambda x: x.startswith(id + "_") and re.search("\d\d\d\d-\d\d-\d\d", x), files)
        if previous:
            dates = [re.search("\d\d\d\d-\d\d-\d\d", x).group(0).split("-") for x in previous]
            dates.sort()
            latest = dates[-1]
            isodate = "-".join(latest)
            foundpath = os.path.join(directory, findthis)
            yesterfiles = self.facility.docs_from_page(open(foundpath).read())
            todayfiles = self.facility.docs_from_page(self.facility.page)
            updated = todayfiles - yesterfiles
        else:
            print "New facility!"
            updated = self.facility.docs_from_page(self.facility.page)
        return updated

    def fetch_all_files_for_facility(self):
        allfiles = set()
        if not self.facility.page:
            self.facility.page = self.retrieve_facility_page()
        rawtypes = re.findall("javascript:addQueryFilter\('xIDEMDocumentType', '([\w\s]+)'\)", self.facility.page)
        types = map(lambda x: x.replace(" ", "%20"), rawtypes)
        print str(types)
        for t in types:
            print "***%s***" % t
            url = generate_type_url(self.facility.vfc_id, t)
            page = urllib2.urlopen(url, timeout=100).read()
            self.facility.page += page
            morefiles = self.fetch_files_for_facility()
            allfiles |= morefiles
            print len(morefiles), len(allfiles), len(self.facility.docs_from_page(self.facility.page)), len(
                self.facility.downloaded_docs)
            time.sleep(5)
        return allfiles

    def fetch_files_for_facility(self):
        allfiles = set()
        files = self.facility.docs_from_page(self.facility.page)
        filenamedic = dict(map(lambda x: (x.filename, x), files))
        filenames = set(filenamedic.keys()) - self.facility.downloaded_docs
        filenames = sorted(list(filenames))
        for filename in filenames:
            doc = filenamedic[filename]
            print doc.filename, len(allfiles), "%d/%d" % (1 + filenames.index(filename), len(filenames))
            doc.path = os.path.join(self.facility.directory, filename)
            if self.whether_download is True:
                doc.retrieve_patiently()
            allfiles.add(doc)
            self.facility.downloaded_docs.add(doc)
        return allfiles

    def scan_zip_for_premature(self):
        sitedirs = filter(lambda x: os.path.isdir(os.path.join(self.directory, x)), os.listdir(self.directory))
        for siteid in sitedirs:
            if siteid not in self.siteids.keys():
                print "%s not in keys!" % siteid
                continue
            self.facility = self.siteids[siteid]
            sitedir = os.path.join(self.directory, siteid)
            if scan_for_premature_stops(sitedir):
                self.get_downloaded_docs()
                total = get_latest_total(sitedir)
                if total > 500:
                    self.fetch_all_files_for_facility()
                else:
                    self.fetch_files_for_facility()
        return True

    def get_facility_from_row(self, row):
        pieces = [x.split("</span>")[0].strip() for x in row.split("<span class=idemfs>")[1:]]
        name, address, city = pieces[:3]  # faciliity, street address, city
        urlpiece = pieces[-1]
        id = urlpiece.split("xAIID<matches>`")[1].split("`")[0]
        site = Facility(row=row, parent=self)
        return site

    def get_facilities_from_page(self, page):
        rows = page.split("<tr>")[1:]
        rows = [x for x in rows if "span class=idemfs" in x]
        rows = [x for x in rows if "xAIID<matches>" in x]
        facilities = map(self.get_facility_from_row, rows)
        return facilities

    def latlongify(self):
        for facility in self.facilities:
            if hasattr(facility, "latlong") and facility.latlong is not False:
                continue
            else:
                try:
                    lat, long, googleaddress = coord_from_address(facility.full_address)
                except TypeError, e:  # returned False?
                    print str(e)
                    pass
                else:
                    print facility.vfc_name, facility.full_address
                    print googleaddress
                    facility.latlong = (float(lat), float(long))
                    facility.address = googleaddress
                    time.sleep(.1)


class Facility:  # data structure
    vfc_name = ""
    vfc_address = ""
    latlong = False
    city = ""
    county = ""
    state = ""
    zip = ""
    vfc_id = ""
    vfc_url = ""
    updated_docs = set()
    downloaded_docs = set()
    directory = ""
    page = ""

    def __init__(self, row=False, parent=False, directory=False, date=False, vfc_id=False):
        if vfc_id is not False:
            self.vfc_id = vfc_id
        if row:  # overrides vfc_id if set
            self.row = row
            self.from_row()
        if parent is not False:
            self.parent = parent
            self.zip = parent.zip
        else:
            print "No parent!", self.vfc_name
        self.set_directory()
        if date is not False:
            self.date = date
        else:
            self.date = datetime.date.today()
        self.set_directory(directory=directory)
        self.docs = set(os.listdir(self.directory))
        self.get_latest_page()
        if not self.page:
            print "retrieving page"
            self.page = self.retrieve_page()
            print len(self.page)
        if not self.downloaded_docs:
            self.downloaded_docs = set(os.listdir(self.directory))
        self.docs = self.docs_from_page(self.page)

    def get_page(self):
        self.page = self.retrieve_page()
        self.docs = self.docs_from_page(self.page)

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

    def from_row(self):
        result = get_individual_site_info(self.row)
        self.vfc_id, self.vfc_name, self.vfc_address, self.city = result
        self.vfc_url = "http://vfc.idem.in.gov/DocumentSearch.aspx?xAIID=" + self.vfc_id
        self.full_address = self.vfc_address + ", " + self.city + self.zip

    def docs_from_page(self, page):
        pattern = '<tr>.+?<a.+?href="(/cs/.+?[^\d](\d{7,9})\.pdf)"[\s\S]+?>(\d+)/(\d+)'
        pattern += '/(\d\d\d\d)<[\s\S]+?nowrap="nowrap">(.+?)</div>[\s\S]+?'
        pattern += 'nowrap="nowrap">(.+?)</div>'
        rows = re.findall(pattern, page)
        docs = [Document(row=x, facility=self) for x in rows]
        return set(docs)

    def download(self):
        allfiles = set()
        filenamedic = dict(map(lambda x: (x.filename, x), self.docs))
        filenames = set(filenamedic.keys()) - self.downloaded_docs
        filenames = sorted(list(filenames))
        for filename in filenames:
            doc = filenamedic[filename]
            print doc.filename, len(allfiles), "%d/%d" % (1 + filenames.index(filename), len(filenames))
            doc.path = os.path.join(self.directory, filename)
            doc.retrieve_patiently()
            allfiles.add(doc)
            self.downloaded_docs.add(doc)
        return allfiles

    def get_latest_page(self):
        logfilter = lambda x: x.startswith(self.vfc_id) and "." not in x
        logpages = filter(logfilter, os.listdir(self.directory))
        if not logpages:
            return ""
        logpages.sort()
        newest = logpages[-1]
        path_to_newest = os.path.join(self.directory, newest)
        self.page = open(path_to_newest).read()
        return self.page

    def get_updated_docs_in_directory(self, fromdate=False, todate=False):
        if not fromdate:
            fromdate = self.parent.date
        if not todate:
            todate = self.parent.date
        docdic = dict(map(lambda x: (x.filename, x), self.docs))
        docfilter = lambda x: os.path.isfile(os.path.join(self.directory, x)) and x.endswith(
            ".pdf") and x in docdic.keys()
        localfiles = filter(docfilter, os.listdir(self.directory))
        updated = set()
        for filename in localfiles:
            doc = docdic[filename]
            filepath = os.path.join(self.directory, filename)
            mtime = os.path.getmtime(filepath)
            mdate = datetime.date.fromtimestamp(mtime)
            if mdate < fromdate:
                continue
            if todate:
                if mdate > todate:
                    continue
            updated.add(doc)
        return updated

    def retrieve_page(self, firsttime=True):
        if firsttime is False and len(self.downloaded_docs) > 0:
            resultcount = 20
        else:
            resultcount = 500
        starturl = "https://ecm.idem.in.gov/cs/idcplg?IdcService=GET_SEARCH_RESULTS"
        starturl += "&QueryText=xAIID+%3Ccontains%3E+`" + self.vfc_id + "`"
        starturl += "&listTemplateId=SearchResultsIDEM&searchFormType=standard"
        starturl += "&SearchQueryFormat=UNIVERSAL&ftx=&AdvSearch=True&ResultCount="
        starturl += str(resultcount) + "&SortField=dInDate&SortOrder=Desc"
        self.page = urllib2.urlopen(starturl, timeout=100).read()
        pagefilename = self.vfc_id + "_" + self.date.isoformat()
        pagepath = os.path.join(self.directory, pagefilename)
        open(pagepath, "w").write(self.page)
        return self.page


class Document:
    url = ""
    crawl_date = False
    file_date = False
    id = ""
    type = ""
    program = ""
    filename = ""
    facility = ""
    path = ""
    filename = ""

    def __init__(self, row=False, facility=False):
        if facility is not False:
            self.facility = facility
        if row is not False:
            self.row = row
            self.from_row(row)
            try:
                self.filename = "%s_%s_%s_%s.pdf" % (self.file_date, self.id, self.program, self.type)
            except AttributeError, e:
                print str(e)
            else:
                self.filename = self.filename.replace("/", "_")

    def __eq__(self, other):
        if type(other) == str:
            return self.filename == other
        else:
            return self.filename == other.filename

    def __hash__(self):
        return hash(self.filename)

    def from_row(self, row):
        relative_url, self.id, month, date, year, self.program, self.type = row
        self.file_date = datetime.date(int(year), int(month), int(date))
        domain = "https://ecm.idem.in.gov"
        self.url = domain + relative_url
        filename = "%s_%s_%s_%s.pdf" % (self.file_date.isoformat(), self.id, self.program, self.type)
        filename = filename.replace("/", "_")
        self.filename = filename

    def retrieve_patiently(self, maxtries=10):
        done = False
        inc = 0
        while not done:
            inc += 1
            if inc > maxtries:
                print "Aborting!"
                return False
            try:
                urllib.urlretrieve(self.url, self.path)
            except Exception, e:
                print str(e)
                time.sleep(100)
            else:
                done = True
                time.sleep(5)
        return True


def build_zip_url(zip):
    zipurl = "https://ecm.idem.in.gov/cs/idcplg?"
    zipurl += "IdcService=GET_IDEM_FACILITY_SEARCH_PAGE&RunFacilitySearch=1"
    zipurl += "&PrimaryName=&LocationAddress=&CityName=&CountyName=&PostalCode="
    zipurl += str(zip)
    zipurl += "&FacilitySortField=PRIMARY_NAME&FacilitySortOrder=ASC&ResultsPerPage=500"
    return zipurl


def since_last_scan(sitedir):
    files = os.listdir(sitedir)
    siteid = os.path.split(sitedir)[-1]
    previous = filter(lambda x: x.startswith(siteid + "_"), files)
    previous = filter(lambda x: re.search("\d\d\d\d-\d\d-\d\d", x), previous)
    previous.sort()
    if not previous:
        return 1000  # a nice big number of days
    last = previous[-1]
    isodate = re.search("\d\d\d\d-\d\d-\d\d", last).group(0)
    date = datetime.datetime.strptime(isodate, "%Y-%m-%d")
    delta = datetime.datetime.today() - date
    return delta.days


def since_last_file(sitedir, download=False):
    default = 10000
    files = os.listdir(sitedir)
    siteid = os.path.split(sitedir)[-1]
    regfiles = [x for x in files if x.endswith("pdf")]
    regfiles.sort()
    regfiles = [x for x in regfiles if re.match("\d\d\d\d-\d\d-\d\d", x)]
    if regfiles:
        last = regfiles[-1]
        isodate = re.search("\d\d\d\d-\d\d-\d\d", last).group(0)
        date = datetime.datetime.strptime(isodate, "%Y-%m-%d")
    else:
        if download:  # download on, no files present
            return default
        else:
            previous = sorted(filter(lambda x: x.startswith(siteid + "_"), files))
            latestcheck = previous[-1]
            page = open(os.path.join(sitedir, latestcheck)).read()
            dates = re.findall("(\d{1,2})\/(\d{1,2})\/(\d{4})", page)
            if not dates:
                return default
            dates = [(int(x[2]), int(x[0]), int(x[1])) for x in dates]  # crazy American date format
            dates.sort()
            latestfile = dates[-1]
            print latestfile
            date = datetime.datetime(latestfile[0], latestfile[1], latestfile[2])
    delta = datetime.datetime.today() - date
    return delta.days


def get_individual_site_info(row):
    pieces = [x.split("</span>")[0].strip() for x in row.split("<span class=idemfs>")[1:]]
    name, address, city = pieces[:3]  # faciliity, street address, city
    urlpiece = pieces[-1]
    id = urlpiece.split("xAIID<matches>`")[1].split("`")[0]
    return id, name, address, city


def get_latest_zip_page(zip, zipdir=False):
    if zipdir is False:
        zipdir = os.path.join(maindir, zip)
    logfilter = lambda x: x.endswith(".html") and x.startswith(zip)
    logpages = filter(logfilter, os.listdir(zipdir))
    logpages.sort()
    newest = logpages[-1]
    path_to_newest = os.path.join(zipdir, newest)
    zippage = open(path_to_newest).read()
    return zippage


def scan_for_premature_stops(sitedir, tolerance=0.05):
    total = get_latest_total(sitedir)
    total = int(total)
    pdfs = filter(lambda x: x.endswith(".pdf"), os.listdir(sitedir))
    if total - (tolerance * total) > len(pdfs):
        print sitedir, total, len(pdfs)
        return True
    else:
        return False


def generate_type_url(id, type):
    urlbase1 = "https://ecm.idem.in.gov/cs/idcplg?IdcService=GET_SEARCH_RESULTS&QueryText=xAIID+%3Ccontains%3E+`"
    urlbase2 = "`&listTemplateId=SearchResultsIDEM&searchFormType=standard&SearchQueryFormat=UNIVERSAL&ftx=&AdvSearch=True&ResultCount=500&SortField=dInDate&SortOrder=Desc&QueryFilter=xIDEMDocumentType%20%3CMatches%3E%20%60"
    urlbase3 = "%60&PageNumber=1&StartRow=1&EndRow=500&FilterFields=xIDEMDocumentType"
    url = urlbase1 + id + urlbase2 + type + urlbase3
    return url


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
    latitude = str(round(float(latitude), 5))
    longitude = geometry.split('"lng"')[1].split(':')[1].split(",")[0].split("\n")[0].strip()
    longitude = str(round(float(longitude), 5))
    googleadd = apipage.split('"formatted_address"')[1].split('"')[1].split('"')[0].strip()
    return (latitude, longitude, googleadd)


### enforcement

def fetch_enforcements(county="Lake", days=90, directory=enforcementdir):
    updates = []
    today = datetime.date.today()
    delta = datetime.timedelta(days)
    start = today - delta
    startday = start.strftime("%d")
    startmonth = start.strftime("%b")
    startyear = start.strftime("%Y")
    nowday = today.strftime("%d")
    nowmonth = today.strftime("%b")
    nowyear = today.strftime("%Y")
    url = "http://www.in.gov/apps/idem/oe/idem_oe_order?"
    url += "company_name=&case_number=&old_case_number=&county=" + county
    url += "&media=All&type=0&start_month=" + startmonth + "&start_day=" + startday
    url += "&start_year=" + startyear + "&end_month=" + nowmonth + "&end_day=" + nowday
    url += "&end_year=" + nowyear + "&page=T&action=Search"
    print url
    page = urllib2.urlopen(url, timeout=100).read()
    filename = today.isoformat() + "_" + county + ".html"
    filepath = os.path.join(directory, filename)
    open(filepath, "w").write(page)
    output = []
    rows = page.split("<TR>")[1:]
    rows = [x for x in rows if '<TD ALIGN="CENTER"' in x]
    for row in rows:
        city = row.split('<TD>&nbsp;<font size="-1">')[2].split("<")[0]
        company = row.split('<font size="-1">', 1)[1].split("<")[0]
        if "href" in row:
            actionurl = row.split('<a href="')[1].split('"')[0]
            basefilename = actionurl.split("/")[-1]
            type = actionurl.split("/")[-2]
            filename = company + "_" + city + "_" + type + "_" + basefilename
        else:
            actionurl = ""
            filename = ""
        output.append((city, company, actionurl, filename))
        if not filename:
            continue
        already = set(os.listdir(directory))
        if filename not in already:
            print filename
            filepath = os.path.join(directory, filename)
            actionpage = urllib2.urlopen(actionurl, timeout=100).read()
            open(filepath, "w").write(actionpage)
            updates.append((today.isoformat(), filename))
    if updates:
        tsv = "\n".join(["\t".join(x) for x in updates])
        writefile = open(os.path.join(directory, "updates_" + today.isoformat() + ".txt"), "w")
        with writefile:
            writefile.write(tsv)
    return updates


# INND

class CaseUpdater:
    def __init__(self):
        pass


def fetch_daily_INND():
    new = collections.defaultdict(list)
    alerts = alerts_from_file()
    alerted = []
    url = "https://ecf.innd.uscourts.gov/cgi-bin/rss_outside.pl"
    directory = innddir
    rss = urllib2.urlopen(url, timeout=100).read()
    items = rss.split("<item>")[1:]
    items = [x.split("</item>")[0] for x in items]
    items.reverse()  # put in chron order
    for item in items:
        for alert in alerts:
            if alert in item:
                print "***" + alert
                print item
                alerted.append(alert)
        title = item.split("<title>")[1].split("</title>")[0]
        title = unescape(title)
        casenum = title.split(" ")[0]
        caselink = item.split("<link>")[1].split("</link>")[0]
        pubdate = item.split("<pubDate>")[1].split("</pubDate>")[0]
        pubdate = unescape(pubdate)
        description = item.split("<description>", 1)[1].split("</description>")[0]
        description = unescape(description)
        if "<a" in description:
            entrynum = description.split("<a")[1].split(">", 1)[1].split("</a>")[0]
        else:
            entrynum = ""
        if "<guid" in item:
            guid = item.split("<guid")[1].split(">", 1)[1].split("</guid>")[0]
            guid = unescape(guid)
        else:
            guid = ""
        new[casenum].append((title, caselink, pubdate, description, entrynum, guid))
    changed = process_case_updates(new)
    return changed, alerted


def alerts_from_file(filepath=False):
    if filepath is False:
        filepath = os.path.join(innddir, "alertme.txt")
    alertfile = open(filepath)
    alerts = []
    for line in alertfile:
        line = line.split("#")[0]
        line = line.strip()
        if not line:
            continue
        alerts.append(line)
    alerts = list(set(alerts))
    alerts.sort()
    return alerts


def process_alerts(directory=innddir):
    oho = []
    filepath = os.path.join(directory, "alertme.txt")
    alerts = alerts_from_file()
    for line in alerts:
        line = line.split("#")[0]
        line = line.strip()
        if not line:
            continue
        if line in str(os.listdir(directory)):
            oho.append(line)
    return oho


def process_case_updates(new, directory=innddir):
    filenames = [casenum + ".html" for casenum in new.keys()]
    already = set(os.listdir(directory))
    changed = []
    for n in filenames:  # creating new files for cases not previously logged
        path = os.path.join(directory, n)
        key = n.replace(".html", "")
        title, caselink, pubdate, description, entrynum, guid = new[key][0]
        if n in already:
            html = open(path).read()
        else:
            html = create_case_HTML(title, caselink)
        original = str(html)
        for title, caselink, pubdate, description, entrynum, guid in new[key]:
            newrow = create_case_row((title, caselink, pubdate, description, entrynum, guid))
            if newrow in html:
                continue
            html = add_row_to_table(html, newrow)
        if original != html:
            open(path, "w").write(html)
            changed.append(n)
    return changed


def create_case_HTML(title, link):
    html = "<html><body>%s%s</body></html>"
    header = '\n<h2><a href="%s">%s</a></h2>\n' % (link, title)
    table = '\n\n<table width="100%">\n<tr>\n<th width="15%" align ="left">Time</th>'
    table += '<th width="10%" align="left">Entry</th>'
    table += '<th align="left">Description</th></tr>\n</table>'
    output = html % (header, table)
    return output


def create_case_row(casetuple):
    newrow = "<tr><td>%s</td><td>%s</d><td>%s</td></tr>\n"
    title, caselink, pubdate, description, entrynum, guid = casetuple
    firstcell = '<a href="%s">%s</a>' % (guid, pubdate)
    secondcell = entrynum
    thirdcell = description
    newrow = newrow % (firstcell, secondcell, thirdcell)
    return newrow


def add_row_to_table(html, newrow):
    html = html.replace("</table>", "\n" + newrow + "</table>")
    return html


def unescape(s):  # ex https://wiki.python.org/moin/EscapingXml
    want_unicode = False
    if isinstance(s, unicode):
        s = s.encode("utf-8")
        want_unicode = True
    list = []
    p = xml.parsers.expat.ParserCreate("utf-8")
    p.buffer_text = True
    p.returns_unicode = want_unicode
    p.CharacterDataHandler = list.append
    p.Parse("<e>", 0)
    p.Parse(s, 0)
    p.Parse("</e>", 1)
    es = ""
    if want_unicode:
        es = u""
    return es.join(list)


### permits

class Permit:
    pm = ""
    facility = ""
    comment_period = ""
    start_date = False
    end_date = False
    county = ""
    type = ""
    url = ""
    row = ""
    more = ""
    number = ""
    row = ""

    def __init__(self, row):
        row = row.replace("&amp;", "&")
        name, type, dates, comment, more = tuple(
            [x.split(">", 1)[1].split("</td>")[0] for x in row.split("<td", 5)[1:]])
        self.facility = name
        self.type = type.split(">")[1].split("[")[0].split("<")[0].strip()
        url = type.split('href="')[1].split('"')[0]
        if url.startswith("/"):
            url = "http://www.in.gov" + url
        self.url = url
        self.more = more
        self.pm = self.extract_info("Project Manager:")
        self.number = self.extract_info("Permit Number:")
        if comment == "Yes":
            self.comment_period = dates
            self.convert_dates()
        self.row = row

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return hash(self.data())

    def __str__(self):
        return "\n".join(["%s: %s" % (x, getattr(self, x)) for x in dir(self) if not callable(x)])

    def data(self):
        data = (self.facility, self.type, self.url, self.pm, self.number, self.county, self.comment_period)
        data = tuple(map(lambda x: str(x), data))
        return data

    def extract_info(self, label):
        if label in self.more:
            value = self.more.split(label)[1]
            value = value.split("<")[0].split("\n")[0].strip()
            return value
        else:
            return False

    def is_comment_open(self, date=datetime.date.today()):
        if self.start_date is False or self.end_date is False:
            return False
        else:
            if date > self.end_date:
                return False
            elif date < self.start_date:
                return False
            else:
                return True

    def convert_dates(self):
        datestring = self.comment_period.strip()
        if not datestring:
            return False
        else:
            pieces = datestring.split(" – ")
            if len(pieces) == 1:
                pieces = datestring.split(" - ")
            start, stop = pieces
            self.start_date = self.convert_single_date(start)
            self.end_date = self.convert_single_date(stop)

    def convert_single_date(self, usdate):
        date = datetime.datetime.strptime(usdate, "%m/%d/%Y").date()
        return date


class PermitUpdater:
    directory = permitdir
    main_url = "http://www.in.gov/idem/6395.htm"
    whether_download = True

    def __init__(self):
        self.date = datetime.date.today()

    def check_new_permits(self):  # return row data for new permits
        page = urllib2.urlopen(self.main_url).read()
        today = datetime.date.today().isoformat()
        filename = self.main_url.split("/")[-1]
        filename = today + "_" + filename
        open(os.path.join(self.directory, filename), "w").write(page)
        return page

    def compare_permits(self, newpage, oldpage):
        newpermits = self.get_permits_from_page(newpage)
        oldpermits = self.get_permits_from_page(oldpage)
        new = newpermits - oldpermits
        old = oldpermits - newpermits
        return new, old

    def get_permits_from_page(self, page):
        permits = set()
        countychunks = page.split('<th class="section" colspan="5">')[1:]
        for c in countychunks:
            countypermits = self.get_permits_from_section(c)
            permits |= countypermits
        return permits

    def get_permits_from_section(self, countychunk):
        permits = set()
        county, countyrows = tuple(countychunk.split("</th>", 1))
        if countyrows.count("</tr>") < 2:
            return set()
        rows_split = countyrows.split("</tr>")[1:-1]
        for row in rows_split:
            if row.count("<td") < 5:
                continue
            newpermit = Permit(row)
            newpermit.county = county
            permits.add(newpermit)
        return permits

    def do_daily_permit_check(self):
        files = os.listdir(self.directory)
        files.sort()
        files = filter(lambda x: x.endswith(".htm"), files)
        latest = files[-1]
        today = datetime.date.today().isoformat()
        if today in latest:  # avoid tripping over self
            if len(files) > 1:
                latest = files[-2]
                print latest
        latestpath = os.path.join(self.directory, latest)
        oldpage = open(latestpath).read()
        newpage = self.check_new_permits()
        self.current = self.get_permits_from_page(newpage)
        self.new, self.old = self.compare_permits(newpage, oldpage)
        logtext = self.log_permit_updates()
        self.download_new_permits()
        return logtext

    def log_permit_updates(self):
        if not self.new and not self.old:
            return ""
        logfilename = "updates_%s.txt" % self.date.isoformat()
        logpath = os.path.join(self.directory, logfilename)
        logtext = ""
        if self.new:
            logtext += "*** New notices today***\n\n"
            for permit in self.new:
                logtext += "\t".join(permit.data()) + "\n"
        if self.old:
            logtext += "\n\n*** Notices removed today***\n\n"
            for permit in self.old:
                logtext += "\t".join(permit.data()) + "\n"
        print logpath
        logfile = open(logpath, "w")
        with logfile:
            logfile.write(logtext)
        return logtext

    def download_new_permits(self):
        if not self.whether_download:
            return False
        for permit in self.new:
            fileurl = permit.url
            filename = fileurl.split("/")[-1]
            filename = self.date.isoformat() + "_" + permit.county + "_" + permit.facility + "_" + filename
            filepath = os.path.join(self.directory, filename)
            urllib.urlretrieve(fileurl, filepath)
        return True

    def get_open_permits(self):
        opens = filter(lambda x: x.is_comment_open(date=self.date), self.current)
        return opens


def daily_permit_check():
    updater = PermitUpdater()
    logtext = updater.do_daily_permit_check()
    return logtext


def get_dirs_from_zip(zip):
    dirs = os.listdir(zip)
    dirs = map(lambda x: os.path.join(zip, x), dirs)
    dirs = filter(lambda x: os.path.isdir(x), dirs)
    return dirs


def count_files_in_zip(zip):
    dirs = get_dirs_from_zip(zip)
    sum = 0
    for d in dirs:
        files = os.listdir(d)
        files = filter(lambda x: x.endswith(".pdf"), files)
        sum += len(files)
    return sum


def get_sites_with_activity(zip, sincedate=datetime.date(2017, 1, 1)):
    dirs = get_dirs_from_zip(zip)
    sites_with_activity = []
    sites = get_sites_from_directory(zip)
    for d in dirs:
        files = os.listdir(d)
        files = filter(lambda x: x.endswith(".pdf") and x.count("_") > 1, files)
        if not files:
            continue
        files.sort()
        latest = files[-1]
        latest_iso_date = latest.split("_")[0]
        year, month, day = tuple(latest_iso_date.split("-"))
        latest_date = datetime.date(int(year), int(month), int(day))
        if latest_date < sincedate:
            continue
        else:
            siteid = os.path.split(d)[-1]
            sites_with_activity.append((siteid, sitenames[siteid]))
    return sites_with_activity


def get_enforcement_address(page):
    page = re.sub("\s+", " ", page)
    if " located at " not in page:
        return False
    addr = page.split(" located at ")[1]
    addr = addr.split(" in ")[0].split(",")[0].split(".")[0]
    addr = re.sub("<.+?>", "", addr)
    return addr


def addrs_are_same(add1, add2):
    if add1 == add2:
        return True
    add1 = normalize_address(add1)
    add2 = normalize_address(add2)
    first_two = lambda x: " ".join(x.split(" ")[:2])
    if first_two(add1) == first_two(add2):
        return True
    return False


def normalize_address(address):
    address = address.upper()
    address = replace_nums(address)
    address = replace_dirs(address)
    return address


def replace_nums(address):
    address = " %s " % address
    nums = {"ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5, "SIX": 6, "SEVEN": 7, "EIGHT": 8, "NINE": 9,
            "TEN": 10}
    for n in nums:
        catchme = " %s " % n
        if catchme in address:
            address = address.replace(catchme, " %s " % str(nums[n]))
    return address.strip()


def replace_dirs(address):
    directions = {" NORTH ": " N ", " SOUTH ": " S ", " EAST ": " E ", " WEST ": " W "}
    for d in directions:
        if d in address:
            address = address.replace(d, directions[d])
    return address.strip()


def get_tops(cutoff=10, maindirectory=maindir, zips=lakezips, years=[]):
    sortable = []
    for zip in zips:
        updater = ZipUpdater(zip)
        facilities = updater.facilities
        for facility in facilities:
            sortable.append(get_sortable_data_for_site(facility, years))
    sortable.sort()
    sortable.reverse()
    tops = sortable[:cutoff]
    return tops


def get_sortable_data_for_site(facility, years=[]):
    if not years:
        count = get_latest_total(facility.directory)
    else:
        count = get_total_by_years(facility.directory, years)
    siteinfo = (count, facility.vfc_name, facility.vfc_id, facility.zip)
    return siteinfo


def get_total_by_years(directory, years):
    yearfilter = lambda x: x.endswith("pdf") and int(x.split("-")[0]) in years
    files = filter(yearfilter, os.listdir(directory))
    return len(files)


def get_latest_total(directory):
    files = os.listdir(directory)
    pdfs = filter(lambda x: x.endswith("pdf"), files)
    isdir = lambda x: os.path.isdir(os.path.join(directory, x))
    pagefilter = lambda x: "." not in x and not isdir(x)
    pages = filter(pagefilter, files)
    if not pages:
        return False
    pages.sort()
    latest = pages[-1]
    latestcontent = open(os.path.join(directory, latest)).read()
    total = get_total_from_page(latestcontent)
    return total


def get_total_from_page(page):
    pattern1 = "Number of items found:\s*(\d+)"
    pattern2 = "Items 1-\d+ of (\d+)"
    pattern3 = "Found (\d+) items"
    done = False
    for pattern in [pattern1, pattern2, pattern3]:
        try:
            total = re.search(pattern, page).group(1)
        except AttributeError:
            continue
        else:
            done = True
            break
    if done is False:
        total = 0
    total = int(total)
    return total


def wp_login():
    from wordpress_xmlrpc import Client
    username = "Testy McTest"
    password = "zTz3NB%kwth%K$YLRG"
    client = Client('http://samhenderson.net/eco/xmlrpc.php', username, password)
    return client


def upload_post_text(text, publish=False, posttitle="", tags=[], slug=""):
    from wordpress_xmlrpc import WordPressPost, Client
    from wordpress_xmlrpc.methods.posts import GetPosts, NewPost, EditPost
    from wordpress_xmlrpc.methods.users import GetUserInfo
    post = WordPressPost()
    client = wp_login()
    print "Logged in..."
    if not posttitle:
        post.title = "Notifications for %s" % datetime.date.today().strftime("%B %d, %Y")
    else:
        post.title = posttitle
    post.content = text
    if tags:
        tags = [x.lower().replace(" ", "-") for x in tags]
        tags = [re.sub("[^\w\-]", "", x) for x in tags]
        print str(tags)
        post.terms_names['post_tag'] = tags
    if not slug:
        slug = posttitle.replace(",", "").replace(" ", "-").lower()
    post.slug = slug
    done = False
    while not done:
        try:
            post.id = client.call(NewPost(post))
        except Exception, e:  # debug tag issue
            print str(e)
            time.sleep(5)
        else:
            done = True
    if publish:
        post.post_status = 'publish'
        client.call(EditPost(post.id, post))
    return post.id


def get_distance(point1,
                 point2):  # ex http://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
    from math import sin, cos, sqrt, atan2, radians
    # approximate radius of earth in km
    R = 6373.0
    lat1 = radians(point1[0])
    lon1 = radians(point1[1])
    lat2 = radians(point2[0])
    lon2 = radians(point2[1])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance
