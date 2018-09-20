# -*- coding: utf-8 -*-

# unit testing
# objects for enforcements
# objects for collections (zips, docs, facs)
# effective cross-referencing
# instant reanimations
# structure for storing facility data independently of source
# inserting links for CAA & NPDES permits
# fix recurrent log glitches

import collections
import datetime
import geojson
from hurry.filesize import size as convert_size
import os
import re
import requests
import shutil
import time
import urllib
import urllib2
import xml.parsers.expat

import idem_settings
import tea_core
from tea_core import TIMEOUT

lakezips = idem_settings.lake_zips
downloadzips = idem_settings.download_zips
maindir = idem_settings.maindir
permitdir = idem_settings.permitdir
enforcementdir = idem_settings.enforcementdir
latlong_filepath = os.path.join(idem_settings.maindir, "facilitydump.txt")
latest_json_path = os.path.join(idem_settings.websitedir, "latest_vfc.json")


class Document:
    url = ""
    crawl_date = None
    file_date = None
    id = ""
    type = ""
    program = ""
    facility = None
    path = ""
    row = ()
    size = 0
    session = None

    def __init__(self,
                 row=None,
                 build=False,
                 **kwargs):
        if row is not None and build is not False:
            self.row = row
            self.from_oldstyle_row(row)
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def __eq__(self, other):
        if self.filename:
            return self.filename == other.filename
        else:
            return hash(self) == hash(other)

    def __lt__(self, other):
        return self.file_date < other.file_date

    def __hash__(self):
        return hash(self.identity)

    @property
    def identity(self):
        if self.filename:
            return self.filename
        else:
            return self.id, self.facility

    @property
    def filename(self):
        if self.file_date:
            date = self.file_date.isoformat()
        else:
            date = "UNDATED"
        attrs = (date, self.id, self.program, self.type)
        filename = "%s_%s_%s_%s.pdf" % attrs
        filename = filename.replace("/", "_")
        return filename

    @property
    def latest_date(self):
        if self.file_date and self.crawl_date:
            return max(self.file_date, self.crawl_date)
        elif self.file_date:
            return self.file_date
        else:
            return self.crawl_date

    def from_oldstyle_row(self, row):
        relative_url, self.id, month, date, year, self.program, self.type, self.size = row
        self.file_date = datetime.date(int(year), int(month), int(date))
        domain = "https://ecm.idem.in.gov"
        self.url = domain + relative_url

    def retrieve_file_patiently(self, maxtries=10):
        done = False
        inc = 0
        while not done:
            inc += 1
            if inc > maxtries:
                print "Aborting!"
                return False
            try:
                response = self.session.get(self.url, stream=True)
                with open(self.path, 'wb') as out_file:
                    shutil.copyfileobj(response.raw, out_file)
            except Exception, e:
                print str(e)
                time.sleep(30)
            else:
                del response
                done = True
                print "Download complete"
                time.sleep(3)
        return True


class Facility:  # data structure
    row = ""
    vfc_name = ""
    vfc_address = ""
    latlong = False
    city = ""
    county = ""
    state = ""
    zip = ""
    full_address = ""
    google_address = ""
    vfc_id = ""
    vfc_url = ""
    directory = ""
    page = ""
    parent = None
    resultcount = 20
    worry_about_crawl_date = True
    filenamedic = None

    def __init__(self, row=None, parent=None, directory=None, date=None, vfc_id=None, retrieve=False, **arguments):
        self.updated_docs = set()
        self.downloaded_filenames = set()
        self.session = requests.Session()
        self.docs = []
        self.doc_ids = set()
        if vfc_id is not False:
            self.vfc_id = vfc_id
        if row:  # overrides vfc_id if set
            self.row = row
            self.from_row()
        if parent is not None:
            self.parent = parent
            self.zip = parent.zip
        else:
            print "No parent!", self.vfc_name
        if date is not None:
            self.date = date
        else:
            self.date = datetime.date.today()
        self.set_directory(directory=directory)
        if arguments:
            tea_core.assign_values(self, arguments, tolerant=True)
        self.downloaded_filenames = self.get_downloaded_docs()
        self.since_last_check = since_last_scan(self.directory)
        self.get_latest_page()
        if retrieve:
            if not self.page:
                print "retrieving page", self.vfc_id, self.vfc_name
                self.page = self.retrieve_page()
                print len(self.page)
        if not self.downloaded_filenames:
            self.downloaded_filenames = self.get_downloaded_docs()
        self.docs = self.docs_from_directory()
        self.build_filenamedic()

    def get_downloaded_docs(self):
        if self.directory:
            docs = set(os.listdir(self.directory))
        else:
            docs = set()
            return docs
        docs = set(filter(lambda x: x.endswith(".pdf"), docs))
        self.downloaded_filenames = docs
        return docs

    @property
    def due_for_download(self):
        if not self.docs:
            return False
        all_filenames = set([x.filename for x in self.docs])
        if all_filenames - self.downloaded_filenames:
            return True
        else:
            return False

    @property
    def programs(self):
        programs = [x.program for x in self.docs]
        programs = set(programs)
        programs = sorted(list(programs))
        return programs

    def is_active_since(self, cutoff_date):
        if not self.docs:
            return False
        if self.latest_file_date >= cutoff_date:
            return True
        elif self.latest_crawl_date >= cutoff_date:
            return True
        else:
            return False

    def set_directory(self, directory=None):
        if directory:
            self.directory = directory
        elif self.parent is not None:
            self.directory = os.path.join(self.parent.directory, self.vfc_id)
        elif self.vfc_id:
            self.directory = self.vfc_id
        else:
            return ""
        if not os.path.isdir(self.directory):
            os.mkdir(self.directory)
        return self.directory

    def from_row(self):
        result = get_individual_site_info(self.row)
        self.vfc_id, self.vfc_name, self.vfc_address, self.city = result
        self.vfc_url = "http://vfc.idem.in.gov/DocumentSearch.aspx?xAIID=" + self.vfc_id
        self.full_address = self.vfc_address + ", " + self.city + self.zip

    def docs_from_directory(self):
        filenames = os.listdir(self.directory)
        if self.worry_about_crawl_date:
            docs = self.docs_from_pages(filenames)
        else:
            self.get_latest_page()
            docs = self.docs_from_page(self.page)
        self.docs = docs
        return docs

    def docs_from_pages(self, filenames):
        all_docs = []
        filenames.sort()  # put in chronological order
        for filename in filenames:
            if not filename:
                continue
            page_docs = self.filename_to_docs(filename)
            for doc in page_docs:
                if doc.id in self.doc_ids:  # already crawled on previous date
                    continue
                else:
                    self.doc_ids.add(doc.id)
                    all_docs.append(doc)
        return all_docs

    def filename_to_docs(self, filename):
        crawl_date_iso = re.search("\d\d\d\d-\d\d-\d\d", filename).group(0)
        filepath = os.path.join(self.directory, filename)
        page = open(filepath).read()
        crawl_date = date_from_iso(crawl_date_iso)
        page_docs = self.docs_from_page(page, crawl_date=crawl_date, skip_ids=self.doc_ids)
        return page_docs

    @staticmethod
    def get_info_from_old_style_rows(page):
        pattern = '<tr>.+?<a.+?href="(/cs/.+?[^\d](\d{7, 9})\.pdf)"[\s\S]+?>(\d+)/(\d+)' \
            '/(\d\d\d\d)<[\s\S]+?nowrap="nowrap">(.+?)<[\s\S]+?nowrap="nowrap">(\d+)</div>[\s\S]+?' \
            'nowrap="nowrap">(.+?)</div>'
        old_style_rows = re.findall(pattern, page)
        return old_style_rows

    def docs_from_page(self, page, crawl_date=None, skip_ids=None):
        if skip_ids is None:
            skip_ids = set()
        # pattern for older (pre-Aug 2018) pages
        docs = []
        old_style_rows = self.get_info_from_old_style_rows(page)
        if old_style_rows:
            for rowdata in old_style_rows:
                docid = rowdata[1]
                if docid in skip_ids:
                    continue
                else:
                    newdoc = Document(row=rowdata, facility=self, crawl_date=crawl_date, session=self.session)
                    docs.append(newdoc)
        # newer pattern
        else:
            rows = page.split("<tr")[1:]
            rows = [x for x in rows if "xuiListContentCell" in x]
            for rowtext in rows:
                linkcatcher = re.search('(\d+)\.pdf"', rowtext)
                if not linkcatcher:
                    continue
                fileid = linkcatcher.group(1)
                if fileid in skip_ids:
                    continue
                else:
                    newdoc = build_document_from_row(rowtext, self, crawl_date=crawl_date)
                    docs.append(newdoc)
        docs.sort()
        return docs

    def build_filenamedic(self):
        filenamedic = dict(map(lambda x: (x.filename, x), self.docs))
        self.filenamedic = filenamedic

    def download(self, filenames=None):
        allfiles = set()
        self.build_filenamedic()
        if filenames is None:
            filenames = set(self.filenamedic.keys()) - self.downloaded_filenames
            filenames = sorted(list(filenames))
        for filename in filenames:
            print filename, "%d/%d" % (1 + filenames.index(filename), len(filenames))
            doc = self.download_filename(filename)
            allfiles.add(doc)
            self.downloaded_filenames.add(filename)
        return allfiles

    def download_filename(self, filename):
        doc = self.filenamedic[filename]
        doc.path = os.path.join(self.directory, filename)
        doc.retrieve_file_patiently()
        return doc

    def is_log_page(self, filename):
        """
        Return True for a log page (no suffix), False otherwise.
        :param filename: str
        :return: bool
        """
        whether_log_page = False
        if filename.startswith(self.vfc_id):
            if "." not in filename:
                whether_log_page = True
        return whether_log_page

    def get_all_filenames(self):
        """
        Provide an unfiltered set of all filenames in the facility directory.
        :return: set
        """
        filenames = os.listdir(self.directory)
        filenames = set(filenames)
        return filenames

    def get_latest_page(self):
        """
        Provide content of latest downloaded log page in facility directory.
        :return: str
        """
        logpages = filter(self.is_log_page, self.get_all_filenames())
        if not logpages:
            return ""
        logpages.sort()
        newest = logpages[-1]
        path_to_newest = os.path.join(self.directory, newest)
        self.page = open(path_to_newest).read()
        return self.page

    @property
    def latest_file_date(self):
        """
        Return date object for the most recent file date of any file associated with the facility.
        :return: datetime.date
        """
        if not self.docs:
            return None
        else:
            latest_doc = self.docs[-1]
            latest_file_date = latest_doc.file_date
            return latest_file_date

    @property
    def latest_crawl_date(self):
        """
        Return date object for the most recent CRAWL date for any file associated with the facility.
        :return: datetime.date
        """
        if not self.docs:
            return None
        else:
            crawls = [x.crawl_date for x in self.docs]
            crawls.sort()
            latest_crawl_date = crawls[-1]
            return latest_crawl_date

    def check_for_new_docs(self, page=None):
        if not page:
            page = self.retrieve_page()
        docs = self.docs_from_page(page, crawl_date=datetime.date.today())
        new_docs = list(set(docs) - set(self.docs))
        self.docs.extend(new_docs)
        new_docs.sort()
        return new_docs

    def get_updated_docs_in_directory(self, fromdate=None, todate=None):
        docdic = dict([(x.filename, x) for x in self.docs])
        if not fromdate:
            fromdate = self.parent.date
        if not todate:
            todate = self.parent.date

        def is_local_pdf(filename):
            whether_local_pdf = False
            if os.path.isfile(os.path.join(self.directory, filename)):
                if filename.endswith(".pdf"):
                    if filename in docdic.keys():
                        whether_local_pdf = True
            return whether_local_pdf
        localfiles = filter(is_local_pdf, os.listdir(self.directory))
        updated = set()
        for localfile in localfiles:
            doc = docdic[localfile]
            filepath = os.path.join(self.directory, localfile)
            mtime = os.path.getmtime(filepath)
            mdate = datetime.date.fromtimestamp(mtime)
            if mdate < fromdate:
                continue
            if todate:
                if mdate > todate:
                    continue
            updated.add(doc)
        return updated

    def retrieve_page(self, firsttime=True, delay=1):
        if firsttime is False and len(self.downloaded_filenames) > 0:
            self.resultcount = 20
        else:
            self.resultcount = 500
        starturl = self.ecm_url
        self.page = self.retrieve_page_patiently(starturl)
        pagefilename = self.vfc_id + "_" + self.date.isoformat()
        pagepath = os.path.join(self.directory, pagefilename)
        open(pagepath, "w").write(self.page)
        time.sleep(delay)
        return self.page

    @property
    def ecm_url(self):
        starturl = idem_settings.ecm_domain \
                   + "/cs/idcplg?IdcService=GET_SEARCH_RESULTS"\
                   + self.vfc_id \
                   + "`&listTemplateId=SearchResultsIDEM&searchFormType=standard" \
                   + "&SearchQueryFormat=UNIVERSAL&ftx=&AdvSearch=True&ResultCount=" \
                   + str(self.resultcount) \
                   + "&SortField=dInDate&SortOrder=Desc"
        return starturl

    def retrieve_page_patiently(self, url):
        page = get_page_patiently(url, session=self.session)
        return page

    def latlongify(self, force=False):
        if hasattr(self, "latlong") and self.latlong is not False:
            if not force:
                return self.latlong
        else:
            result = coord_from_address(self.full_address)
            try:
                lat, lon, googleaddress = result
            except TypeError, e:  # returned False?
                print str(e)
                print str(result)[:100]
                return False
            else:
                self.latlong = (float(lat), float(lon))
                self.google_address = googleaddress
                return self.latlong

    @property
    def whether_to_update(self):
        maximum_days = 100
        magic_ratio = 4
        sincecheck = self.since_last_check
        if self.latest_file_date:
            sincenewfile = datetime.date.today() - self.latest_file_date
            sincegoodcrawl = datetime.date.today() - self.latest_crawl_date
            sincenewfile = min(sincenewfile.days, sincegoodcrawl.days, maximum_days)
        else:
            sincenewfile = maximum_days
        should_update = True
        if sincecheck < 1:
            should_update = False
        elif sincenewfile / sincecheck > magic_ratio:
            should_update = False
        return should_update


class ZipUpdater:

    def __init__(self, zipcode, **arguments):
        self.zip = zipcode
        self.html = ""
        self.whether_download = True
        self.whether_update_zip_info = True
        self.whether_update_facility_info = True
        self.worry_about_crawl_date = True
        self.offline = False
        self.firsttime = True
        self.directory = os.path.join(maindir, zipcode)
        self.date = datetime.date.today()
        self.zipurl = build_zip_url(zipcode)
        try:
            os.mkdir(self.directory)
        except OSError:  # directory exists
            pass
        tea_core.assign_values(self, arguments, tolerant=True)
        self.page = get_latest_zip_page(self.zip, zipdir=self.directory)
        self.facilities = self.get_facilities_from_page(self.page)
        self.siteids = [(x.vfc_id, x) for x in self.facilities]
        self.sites_by_siteid = dict(self.siteids)
        self.session = requests.Session()
        self.current_facility = None
        self.updated_facilities = []
        self.logtext = ""

    def go_offline(self):
        self.whether_download = False
        self.whether_update_zip_info = False
        self.whether_update_facility_info = False
        self.offline = True

    def do_zip(self):
        if self.whether_update_zip_info is True:
            self.update_info()
        self.get_updated_facilities()
        self.log_updates_ecm()

    def update_info(self):
        print "Updating ZIP info"
        self.retrieve_zip_page()
        self.facilities = self.get_facilities_from_page(self.page)
        self.sites_by_siteid = dict(map(lambda x: (x.vfc_id, x), self.facilities))

    def get_active_sites(self, lookback=7):
        sincedate = get_reference_date(lookback)
        sitelist = self.facilities
        active_sites = get_sites_with_activity(sitelist, sincedate)
        return active_sites

    def retrieve_zip_page(self):
        zippage = get_page_patiently(self.zipurl, session=self.session)
        if self.need_to_get_second_page(zippage):
            nextpage = self.retrieve_second_page()
            zippage += nextpage
        self.save_zip_page(zippage)
        return zippage

    @staticmethod
    def need_to_get_second_page(zippage):
        matchme = "Displaying Facilities 1 - (\d+) of (\d+)"
        matched = re.search(matchme, zippage)
        if matched:
            thispagecount, totalcount = matched.group(1), matched.group(2)
            if int(thispagecount) < int(totalcount):
                return True
        return False

    def save_zip_page(self, zippage):
        zipfilename = str(self.zip) + "_" + self.date.isoformat() + ".html"
        zippagepath = os.path.join(self.directory, zipfilename)
        open(zippagepath, "w").write(zippage)
        self.page = zippage
        return zippage

    def retrieve_second_page(self):
        print "fetching page 2..."  # nothing currently gets close to page 3
        nexturl = self.zipurl + "&PageNumber=2"
        nextpage = get_page_patiently(nexturl, session=self.session)
        return nextpage

    def refresh_siteids(self):
        siteids = sorted(self.sites_by_siteid.keys())
        self.siteids = siteids
        return siteids

    def get_updated_facilities(self):
        self.refresh_siteids()
        self.updated_facilities = []
        for site_id in self.siteids:
            self.handle_facility(site_id)
        return self.updated_facilities

    def handle_facility(self, site_id):
        facility = self.sites_by_siteid[site_id]
        self.current_facility = facility
        if self.whether_update_facility(facility):
            self.update_facility(facility)

    def update_facility(self, facility):
        self.show_progress(facility.vfc_id)
        self.fetch_facility_docs()
        time.sleep(tea_core.DEFAULT_SHORT_WAIT)
        if facility.updated_docs:
            print len(facility.updated_docs)
            self.updated_facilities.append(facility)
            if self.whether_download is True:
                time.sleep(tea_core.DEFAULT_WAIT)

    def show_progress(self, site_id):
        progress = "%d/%d" % (self.siteids.index(site_id) + 1, len(self.siteids))
        print self.current_facility.vfc_name, self.current_facility.vfc_id, progress

    def whether_update_facility(self, facility):  # todo: harmonize with facility.whether_to_update()
        should_update = True
        sincecheck = since_last_scan(facility.directory)
        sincenewfile = since_last_file(facility.directory, download=self.whether_download)
        if self.firsttime is False and self.whether_update_facility_info is True:  # if no updating no need to skimp
            should_update = self.whether_updatable_by_dates(sincecheck, sincenewfile)
        return should_update

    @staticmethod
    def whether_updatable_by_dates(sincecheck, sincenewfile):
        if sincecheck < 1:
            should_update = False
        elif sincenewfile > 60 and sincecheck < 3:  # somewhat arbitrary numbers here
            should_update = False
        elif sincenewfile > 365 and sincecheck < 10:
            should_update = False
        elif sincenewfile > 1500 and sincecheck < 30:
            should_update = False
        else:
            should_update = True
        return should_update

    def log_updates_ecm(self):
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
            for newfile in facility.updated_docs:
                newline = "%s\t%s\t%s\t%s\t%s\n" % (
                    newfile.file_date.isoformat(),
                    facility.vfc_id,
                    facility.vfc_name,
                    newfile.filename,
                    newfile.url)
                text += newline
        return text

    def reconstruct_site_list(self):
        self.page = get_latest_zip_page(self.zip, zipdir=self.directory)
        sites_from_page = self.get_facilities_from_page(self.page)
        siteids = dict(map(lambda x: (x.vfc_id, x), sites_from_page))
        self.sites_by_siteid = siteids
        return siteids

    def get_downloaded_docs(self):
        already = self.current_facility.get_downloaded_docs()
        return already

    def fetch_facility_docs(self):
        if self.whether_update_facility_info is True:
            page = self.retrieve_facility_page()
        else:
            page = self.current_facility.get_latest_page()
        self.current_facility.page = page
        self.current_facility.updated_docs |= self.fetch_files_for_current_facility()
        return self.current_facility.updated_docs

    def retrieve_facility_page(self):
        starturl = self.current_facility.ecm_url
        page = get_page_patiently(starturl, session=self.session)
        self.current_facility.page = page
        pagefilename = self.current_facility.vfc_id + "_" + self.date.isoformat()
        pagepath = os.path.join(self.current_facility.directory, pagefilename)
        open(pagepath, "w").write(page)
        return page

    def build_start_url(self, resultcount=20):
        starturl = idem_settings.ecm_domain
        starturl += "/cs/idcplg?IdcService=GET_SEARCH_RESULTS"
        starturl += "&QueryText=xAIID+%3Ccontains%3E+`" + self.current_facility.vfc_id + "`"
        starturl += "&listTemplateId=SearchResultsIDEM&searchFormType=standard"
        starturl += "&SearchQueryFormat=UNIVERSAL&ftx=&AdvSearch=True&ResultCount="
        starturl += str(resultcount) + "&SortField=dInDate&SortOrder=Desc"
        return starturl

    def check_and_make_facility_directory(self):
        self.current_facility.directory = os.path.join(self.directory, self.current_facility.vfc_id)
        try:
            os.mkdir(self.current_facility.directory)
        except OSError:
            pass
        else:
            self.firsttime = True  # if directory not created, we know it hasn't been checked before
        return self.current_facility.directory

    def get_updated_files_without_downloading(self):
        def date_filter(filename):
            is_previous = False
            prefix = self.current_facility.vfc_id + "_"
            if filename.startswith(prefix) and re.search("\d\d\d\d-\d\d-\d\d", filename):
                is_previous = True
            return is_previous
        directory = self.directory
        files = os.listdir(directory)
        previous = filter(date_filter, files)
        if previous:
            dates = [(re.search("\d\d\d\d-\d\d-\d\d", x).group(0).split("-"), x) for x in previous]
            dates.sort()
            foundpath = os.path.join(directory, dates[-1][-1])
            yesterfiles = self.current_facility.docs_from_page(open(foundpath).read())
            todayfiles = self.current_facility.docs_from_page(self.current_facility.page)
            updated = todayfiles - yesterfiles
        else:
            print "New facility!"
            updated = self.current_facility.docs_from_page(self.current_facility.page)
        return updated

    def fetch_all_files_for_facility(self):
        allfiles = set()
        if not self.current_facility.page:
            self.current_facility.page = self.retrieve_facility_page()
        needle = "javascript:addQueryFilter\('xIDEMDocumentType', '([\w\s]+)'\)"
        rawtypes = re.findall(needle, self.current_facility.page)
        types = map(lambda x: x.replace(" ", "%20"), rawtypes)
        print str(types)
        for t in types:
            morefiles = self.fetch_type_files(t)
            allfiles |= morefiles
            print len(morefiles), len(allfiles)
            time.sleep(tea_core.DEFAULT_WAIT)
        return allfiles

    def fetch_type_files(self, filetype):
        print "***%s***" % filetype
        url = generate_type_url(self.current_facility.vfc_id, filetype)
        page = get_page_patiently(url, session=self.session)
        self.current_facility.page += page
        morefiles = self.fetch_files_for_current_facility()
        return morefiles

    def fetch_files_for_current_facility(self):
        newfiles = self.current_facility.download()
        return newfiles

    def scan_zip_for_premature(self):
        sitedirs = filter(lambda x: os.path.isdir(os.path.join(self.directory, x)), os.listdir(self.directory))
        for siteid in sitedirs:
            if siteid not in self.sites_by_siteid.keys():
                print "%s not in keys!" % siteid
                continue
            self.current_facility = self.sites_by_siteid[siteid]
            sitedir = os.path.join(self.directory, siteid)
            if scan_for_premature_stops(sitedir):
                self.get_downloaded_docs()
                total = get_latest_total(sitedir)
                if total > 500:
                    self.fetch_all_files_for_facility()
                else:
                    self.fetch_files_for_current_facility()
        return True

    def scan_site_for_premature(self, siteid):
        if siteid not in self.sites_by_siteid.keys():
            print "%s not in keys!" % siteid
            return
        self.current_facility = self.sites_by_siteid[siteid]
        sitedir = os.path.join(self.directory, siteid)
        whether_premature = scan_for_premature_stops(sitedir)
        if whether_premature:
            self.get_downloaded_docs()
            total = get_latest_total(sitedir)
            if total > 500:
                self.fetch_all_files_for_facility()
            else:
                self.fetch_files_for_current_facility()

    def get_facility_from_row(self, row):
        facility = Facility(row=row, parent=self, worry_about_crawl_date=self.worry_about_crawl_date)
        return facility

    @staticmethod
    def get_valid_rows_from_page(page):
        rows = page.split("<tr>")[1:]
        rows = [x for x in rows if "span class=idemfs" in x]
        rows = [x for x in rows if "xAIID<matches>" in x]
        return rows

    def get_facilities_from_page(self, page=None):
        if page is None:
            page = self.page
        rows = self.get_valid_rows_from_page(page)
        facilities = map(self.get_facility_from_row, rows)
        return facilities

    def latlongify(self):
        for facility in self.facilities:
            if not facility.latlong:
                facility.latlongify()
            time.sleep(tea_core.DEFAULT_SHORT_WAIT)


class DocumentCollection(list):
    """
    Ordered collection of unique VFC documents.
    """
    def __init__(self, *args):
        self.items = set()
        self.programs = set()
        self.types = set()
        super(DocumentCollection, self).__init__(*args)
        self.recalculate()
        for item in self.items:
            self.validate_item(item)
            self.remove_extra(item)

    def __delitem__(self, key):
        del self[key]
        self.recalculate()

    def remove_extra(self, item):
        if self.count(item) > 1:
            i = 0
            count = 0
            while i < len(self):
                this_item = self[i]
                if this_item == item:
                    count += 1
                    if count > 1:
                        del self[i]
                        continue  # do not increment, so as not to skip decremented next item
                    else:
                        i += 1
                else:
                    i += 1

    def recalculate(self):
        self.items = set(self)
        programs = set()
        types = set()
        for item in self.items:
            programs.add(item.program)
            types.add(item.type)

    def validate_item(self, obj):
        if not isinstance(obj, Document):
            return False
        elif obj in self.items:
            return False
        else:
            return True

    def do_addition(self, obj):
        self.programs.add(obj.program)
        self.types.add(obj.type)

    def append(self, obj):
        self.validate_item(obj)
        super(DocumentCollection, self).append(obj)
        self.do_addition(obj)

    def extend(self, iterable):
        iterable = [x for x in iterable if self.validate_item(x)]
        super(DocumentCollection, self).extend(iterable)
        for i in iterable:
            self.do_addition(i)


class FacilityCollection(list):

    def __init__(self, *args):
        super(FacilityCollection, self).__init__(*args)
        self.iddic = {}
        self.namedic = collections.defaultdict(list)
        self.recalculate()

    def validate_item(self, obj):
        if not isinstance(obj, Facility):
            return False
        elif obj in self:
            return False
        else:
            return True

    def recalculate(self):
        self.remove_extras()
        self.iddic = dict([(x.vfc_id, x) for x in self])
        self.namedic = collections.defaultdict(list)
        for facility in self:
            self.namedic[facility.vfc_name].append(facility)

    def do_addition(self, obj):
        self.iddic[obj.vfc_id] = obj
        self.namedic[obj.vfc_name].append(obj)

    def append(self, obj):
        if self.validate_item(obj):
            super(FacilityCollection, self).append(obj)
            self.do_addition(obj)

    def extend(self, iterable):
        iterable = [x for x in iterable if self.validate_item(x)]
        super(FacilityCollection, self).extend(iterable)
        for i in iterable:
            self.do_addition(i)

    def __delitem__(self, key):
        super(FacilityCollection, self).__delitem__(key)
        self.recalculate()

    def remove_extra_item(self, item):
        if self.count(item) > 1:
            i = 0
            count = 0
            while i < len(self):
                this_item = self[i]
                if this_item == item:
                    count += 1
                    if count > 1:
                        del self[i]
                        continue  # do not increment, so as not to skip decremented next item
                    else:
                        i += 1
                else:
                    i += 1

    def remove_extras(self):
        items = set(self)
        for item in items:
            self.remove_extra_item(item)


class ZipCollection(list):
    html = ""
    update_all = False
    worry_about_crawl_date = True
    restart = False

    def __init__(self, zips=lakezips, offline=False, **kwargs):
        super(ZipCollection, self).__init__()
        self.date = datetime.date.today()
        self.facilities = set()
        self.iddic = {}
        self.namedic = collections.defaultdict(list)
        self.zips = zips
        self.offline = offline
        for zipcode in zips:
            print zipcode
            self.setup_updater(zipcode, kwargs)

    def setup_updater(self, zipcode, kwargs):
        updater = ZipUpdater(zipcode, **kwargs)
        if self.offline:
            updater.go_offline()
        else:
            updater.whether_update_zip_info = (self.date.day % 7 == int(zipcode) % 7)  # to prevent bunching up
            updater.whether_download = zipcode in downloadzips
        for facility in updater.facilities:
            self.add_facility(facility)
        self.append(updater)

    def add_facility(self, facility):
        self.facilities.add(facility)
        self.iddic[facility.vfc_id] = facility
        self.namedic[facility.vfc_name].append(facility)

    def go(self, restart=False):
        if restart:
            self.restart = restart
        for updater in self:
            self.run_updater(updater)

    def run_updater(self, updater):
        if self.restart:
            if updater.zip == self.restart:
                self.restart = False
            else:
                return
        print "***%s***" % updater.zip
        updater.do_zip()

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

    def dump_latlongs(self, filepath=latlong_filepath):
        output = ""
        for facility in self.facilities:
            line = self.generate_facility_line(facility)
            output += line + "\n"
        open(filepath, "w").write(output)

    def generate_facility_line(self, facility):
        facility_id = facility.vfc_id
        name = facility.vfc_name
        address = facility.google_address
        lat, lon = self.stringify_latlong(facility.latlong)
        data = [facility_id, name, lat, lon, address]
        line = "\t".join(data)
        return line

    @staticmethod
    def stringify_latlong(latlong):
        """
        Take latlong (tuple or False) and return as pair of strings
        :param latlong: tuple
        :return: str
        """
        if type(latlong) == tuple:
            lat = str(latlong[0])
            lon = str(latlong[1])
        else:  # e.g. if False
            lat = ""
            lon = ""
        return lat, lon

    def reload_latlongs(self, filepath=latlong_filepath):
        for line in open(filepath):
            if not line.strip() or "\t" not in line:
                continue
            self.assign_geodata_from_line(line)

    def assign_geodata_from_line(self, line):
        facility_id, name, latlong, address = process_location_line(line)
        facility = self.iddic[facility_id]
        facility.latlong = latlong
        facility.google_address = address
        return facility

    def get_all_docs_in_range(self, start_date, end_date):
        docs_in_range = {}
        for facility in self.facilities:
            in_range = [x for x in facility.docs if end_date >= x.file_date >= start_date]
            if in_range:
                docs_in_range[facility] = in_range
        return docs_in_range

    def get_active_facilities(self, lookback_days=7):
        cutoff_date = get_reference_date(lookback_days)
        active_facilities = self.get_active_since(cutoff_date)
        return active_facilities

    def get_active_since(self, cutoff_date):
        active_facilities = []
        for facility in self.facilities:
            if facility.is_active_since(cutoff_date):
                active_facilities.append(facility)
        return active_facilities

    def get_facilities_due_for_look(self):
        to_do = []
        for facility in self.facilities:
            if facility.whether_to_update:
                to_do.append(facility)
        return to_do

    def latlongify(self):
        for facility in self.facilities:
            if not facility.latlong:
                facility.latlongify()

    def catchup_downloads(self):
        for facility in self.facilities:
            self.catchup_facility(facility)

    @staticmethod
    def catchup_facility(facility):
        facility.get_downloaded_docs()
        if facility.due_for_download is True:
            print facility.vfc_name, facility.directory, facility.full_address
            facility.download()
            time.sleep(tea_core.DEFAULT_SHORT_WAIT)


class ZipCycler:

    def __init__(self, zips=lakezips):
        self.zips = zips
        self.new = []
        self.updated = []
        self.iddic = {}
        self.ids = set()

    def cycle(self, do_all=False):
        for current_zip in self.zips:  # avoid holding multiple updaters in memory
            updater = ZipUpdater(current_zip)
            print current_zip, len(updater.facilities)
            for facility in updater.facilities:
                if do_all or facility.whether_to_update:
                    self.update_facility(facility)
        return self.new

    def update_facility(self, facility):
        new_files = facility.check_for_new_docs()
        print facility.vfc_name, len(new_files)
        if new_files:
            self.new.append(new_files)
            self.updated.append(facility)

    def latlongify_updated(self, filepath=latlong_filepath):
        self.iddic = dict([(x.vfc_id, x) for x in self.updated])
        self.ids = set(self.iddic.keys())
        for line in open(filepath):
            self.latlongify_facility_from_line(line)

    def latlongify_facility_from_line(self, line):
        data = process_location_line(line)
        if data is None:
            return
        else:
            self.assign_geodata(data)

    def assign_geodata(self, data):
        facility_id, name, latlong, address = data
        if facility_id in self.ids:
            facility = self.iddic[facility_id]
            facility.latlong = latlong
            facility.google_address = address


def process_location_line(line):
    idcol = 0
    namecol = 1
    latcol = 2
    loncol = 3
    addcol = 4
    if not line.strip() or "\t" not in line:
        return
    pieces = line.split("\t")
    facility_id = pieces[idcol]
    latstring = pieces[latcol]
    lonstring = pieces[loncol]
    name = pieces[namecol]
    address = pieces[addcol].strip()  # avoid trailing newline
    latlong = destring_latlong(latstring, lonstring)
    return facility_id, name, latlong, address


def get_location_data(filepath=latlong_filepath):
    data = []
    for line in open(filepath):
        linedata = process_location_line(line)
        data.append(linedata)
    return data


def destring_latlong(latstring, lonstring):
    if not latstring or not lonstring:
        latlong = False
    else:
        lat = float(latstring)
        lon = float(lonstring)
        latlong = (lat, lon)
    return latlong


def build_zip_url(zipcode):
    zipurl = "https://ecm.idem.in.gov/cs/idcplg?"
    zipurl += "IdcService=GET_IDEM_FACILITY_SEARCH_PAGE&RunFacilitySearch=1"
    zipurl += "&PrimaryName=&LocationAddress=&CityName=&CountyName=&PostalCode="
    zipurl += str(zipcode)
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
    date = date_from_iso(isodate)
    delta = datetime.date.today() - date
    return delta.days


def date_from_iso(isodate):
    date_and_time = datetime.datetime.strptime(isodate, "%Y-%m-%d")
    date = date_and_time.date()
    return date


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
        date = datetime.datetime.strptime(isodate, "%Y-%m-%d").date()
    else:
        if download:  # download on, no files present
            return default
        else:
            def filter_previous(filename):
                prefix = siteid + "_"
                if filename.startswith(prefix):
                    return True
                else:
                    return False
            previous = sorted(filter(filter_previous, files))
            if not previous:
                return default
            latestcheck = previous[-1]
            page = open(os.path.join(sitedir, latestcheck)).read()
            dates = re.findall("(\d{1, 2})/(\d{1, 2})/(\d{4})", page)
            if not dates:
                return default
            dates = [(int(x[2]), int(x[0]), int(x[1])) for x in dates]  # crazy American date format
            dates.sort()
            latestfile = dates[-1]
            print latestfile
            date = datetime.date(latestfile[0], latestfile[1], latestfile[2])
    delta = datetime.date.today() - date
    return delta.days


def get_individual_site_info(row):
    pieces = [x.split("</span>")[0].strip() for x in row.split("<span class=idemfs>")[1:]]
    name, address, city = pieces[:3]  # faciliity, street address, city
    urlpiece = pieces[-1]
    siteid = urlpiece.split("xAIID<matches>`")[1].split("`")[0]
    return siteid, name, address, city


def get_latest_zip_page(zipcode, zipdir=False, num_back=1):
    """
    :param zipcode: str
    :param zipdir: str
    :param num_back: int
    :return: str
    """
    def is_zip_log_file(filename):
        whether_log = False
        if filename.endswith(".html") and filename.startswith(zipcode):
            whether_log = True
        return whether_log
    if zipdir is False:
        zipdir = os.path.join(maindir, zipcode)
    logpages = filter(is_zip_log_file, os.listdir(zipdir))
    logpages.sort()
    if len(logpages) < num_back:
        return ""
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


def generate_type_url(facility_id, document_type):
    urlbase1 = "https://ecm.idem.in.gov/cs/idcplg?IdcService=GET_SEARCH_RESULTS&QueryText=xAIID+%3Ccontains%3E+`"
    urlbase2 = "`&listTemplateId=SearchResultsIDEM&searchFormType=standard&SearchQueryFormat=UNIVERSAL&ftx=&" \
        + "AdvSearch=True&ResultCount=500&SortField=dInDate&SortOrder=Desc&" \
        + "QueryFilter=xIDEMDocumentType%20%3CMatches%3E%20%60"
    urlbase3 = "%60&PageNumber=1&StartRow=1&EndRow=500&FilterFields=xIDEMDocumentType"
    url = urlbase1 + facility_id + urlbase2 + document_type + urlbase3
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
    return latitude, longitude, googleadd


def unescape(s):  # ex https://wiki.python.org/moin/EscapingXml
    want_unicode = False
    if isinstance(s, unicode):
        s = s.encode("utf-8")
        want_unicode = True
    character_list = []
    parser = xml.parsers.expat.ParserCreate("utf-8")
    parser.buffer_text = True
    parser.returns_unicode = want_unicode
    parser.CharacterDataHandler = character_list.append
    parser.Parse("<e>", 0)
    parser.Parse(s, 0)
    parser.Parse("</e>", 1)
    joiner = ""
    if want_unicode:
        joiner = u""
    return joiner.join(character_list)


def get_dirs_from_zip(zipcode):
    dirs = os.listdir(zipcode)
    dirs = map(lambda x: os.path.join(zipcode, x), dirs)
    dirs = filter(lambda x: os.path.isdir(x), dirs)
    return dirs


def count_files_in_zip(zipcode):
    dirs = get_dirs_from_zip(zipcode)
    total = 0
    for d in dirs:
        files = os.listdir(d)
        files = filter(lambda x: x.endswith(".pdf"), files)
        total += len(files)
    return total


def get_sites_with_activity(sitelist, sincedate=datetime.date(2018, 1, 1)):
    sites_by_activity = []
    for site in sitelist:
        activity = len([x for x in site.docs if x.file_date > sincedate or x.crawl_date > sincedate])
        if not activity:
            continue
        else:
            sites_by_activity.append((activity, site))
    sites_by_activity.sort()
    sites_by_activity.reverse()
    sites_with_activity = [x[1] for x in sites_by_activity]
    return sites_with_activity


def addrs_are_same(add1, add2):
    def get_first_two(x):
        return " ".join(x.split(" ")[:2])
    if add1 == add2:
        return True
    else:
        add1 = normalize_address(add1)
        add2 = normalize_address(add2)
        if get_first_two(add1) == get_first_two(add2):
            return True
        else:
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


def get_tops(cutoff=10, zips=lakezips, years=None):
    sortable = []
    if not years:
        years = []
    for zipcode in zips:
        updater = ZipUpdater(zipcode)
        facilities = updater.facilities
        for facility in facilities:
            sortable.append(get_sortable_data_for_site(facility, years))
    sortable.sort()
    sortable.reverse()
    tops = sortable[:cutoff]
    return tops


def get_sortable_data_for_site(facility, years=None):
    if not years:
        count = get_latest_total(facility.directory)
    else:
        count = get_total_by_years(facility.directory, years)
    siteinfo = (count, facility.vfc_name, facility.vfc_id, facility.zip)
    return siteinfo


def get_total_by_years(directory, years):
    """
    :param directory: str
    :param years: list
    :return: list
    """
    def is_from_years(filename):
        whether_from_years = False
        if filename.endswith(".pdf"):
            year = filename.split("-")[0]
            if year in years:
                whether_from_years = True
        return whether_from_years
    files = filter(is_from_years, os.listdir(directory))
    return len(files)


def get_latest_total(directory):
    def filter_pages(filename):
        whether_page = False
        if "." not in filename:
            if not os.path.isdir(os.path.join(directory, filename)):
                whether_page = True
        return whether_page
    files = os.listdir(directory)
    pages = filter(filter_pages, files)
    if not pages:
        return False
    else:
        pages.sort()
        latest = pages[-1]
        latestcontent = open(os.path.join(directory, latest)).read()
        total = get_total_from_page(latestcontent)
        return total


def get_total_from_page(page):
    pattern1 = "Number of items found:\s*(\d+)"
    pattern2 = "Items 1-\d+ of (\d+)"
    pattern3 = "Found (\d+) items"
    found_total = "0"
    for pattern in [pattern1, pattern2, pattern3]:
        search = re.search(pattern, page)
        if search:
            found_total = search.group(1)
            break
    total = int(found_total)
    return total


def get_distance(point1, 
                 point2):
    # ex http://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
    from math import sin, cos, sqrt, atan2, radians
    # approximate radius of earth in km
    radius = 6373.0
    lat1 = radians(point1[0])
    lon1 = radians(point1[1])
    lat2 = radians(point2[0])
    lon2 = radians(point2[1])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = radius * c
    return distance


def get_doc_date(doc):
    date = doc.latest_date
    return date


def get_latest_activity(facility):
    docs = sorted(facility.docs, key=get_doc_date)
    docs.reverse()
    latest = docs[0].file_date
    return latest.isoformat()


def get_docs_since(facility, reference_date):
    activity = []
    for doc in facility.docs:
        date = get_doc_date(doc)
        if date >= reference_date:
            activity.append(doc)
    activity.reverse()
    return activity


def is_doc_fresh(doc, today=None, cutoff=1):
    if today is None:
        today = datetime.date.today()
    delta = datetime.timedelta(cutoff)
    if today - doc.latest_date <= delta:
        return True
    else:
        return False


def build_doc_list_item(doc):
    pattern = '\n<li><a href="%s" target="blank">%s</a> (%s), %s</li>'
    boldpattern = '\n<li><b><a href="%s" target="blank">%s</a> (%s)</b>, %s</li>'
    url = doc.url
    date = tea_core.give_us_date(doc.file_date)
    parenthetical = doc.program + "-" + doc.type
    if doc.size is None:
        doc.size = 0
    size = convert_size(doc.size)
    if is_doc_fresh(doc):
        pattern = boldpattern
    docstring = pattern % (url, date, parenthetical, size)
    return docstring


def build_doc_list(facility, reference_date, cutoff=5):
    docs = get_docs_since(facility, reference_date)
    reference_date_us = tea_core.give_us_date(reference_date)
    doclist = "New docs added since %s:<ul>" % reference_date_us
    count = 0
    for doc in docs:
        count += 1
        if count > cutoff:
            break
        docstring = build_doc_list_item(doc)
        doclist += docstring
    doclist += "\n</ul>"
    if count > cutoff:
        extra_count = len(docs) - cutoff
        doclist += '<br/><a href="%s" target="blank">and %d more</a>' % (facility.ecm_url, extra_count)
    return doclist


def facility_to_html(facility, reference_date=None, add_icons=False):
    if reference_date is None:
        reference_date = get_reference_date()
    url = facility.ecm_url
    name = facility.vfc_name
    address = facility.full_address
    linkline = '<a href="%s" target="blank">%s</a>' % (url, name)
    description = address
    lines = [linkline, description]
    if add_icons is True:
        icondic = {"DW": ("/drinking_water.png", "Drinking water"),
                   "OAQ": ("/smokestack.png", "Air quality"),
                   "UST": ("/Oil_drop.png", "Underground storage tank"),
                   "LUST": ("/Oil_drop.png", "Leaking underground tank"),
                   "HW Site": ("/oilbarrel.png", "Hazardous waste"),
                   "OWQ Wastewater": ("/sewage.png", "Wastewater"),
                   "CRTK": ("/lightbulb.png", "Right to know"),
                   "DW Permits": ("/drinking_water.png", "Drinking water"),
                   "ELTF": ("/money.png", "Excess liability trust fund"),
                   "PCB": ("/skull.png", "PCBs"),
                   "DW Ground Water": ("/drinking_water.png", "Drinking water"),
                   "Contracts": ("/money.png", "Contracts"),
                   "OLQ": ("/dumptruck.png", "Land quality"),
                   "Septage": ("/sewage.png", "Septage"),
                   "Biosolids": ("/sewage.png", "Biosolids"),
                   "SRF": ("/money.png", "State revolving fund"),
                   "State Cleanup": ("/backhoe.png", "State Cleanup"),
                   "Storm Water Industrial": ("/sewage.png", "Storm Water Industrial"),
                   "VRP": ("/backhoe.png", "Voluntary remediation program"),
                   "DW Field Inspections": ("/drinking_water.png", "Drinking water"),
                   "Brownfields": ("/backhoe.png", "Brownfields"),
                   "Site Investigation": ("/investigation.png", "Site investigation"),
                   "Superfund": ("/skull.png", "Superfund"),
                   "OAQ Asbestos": ("/gasmask.png", "OAQ Asbestos"),
                   "Storm Water Construction": ("/sewage.png", "Storm Water Construction"),
                   "DW Compliance": ("/drinking_water.png", "Drinking water"),
                   "Emergency Response": ("/skull.png", "Emergency Response"),
                   "OAQ Air Monitoring": ("/smokestack.png", "Air monitoring"),
                   "SW Facility": ("/dumptruck.png", "Solid waste facility"),
                   }
        iconline = '<p class="icon-line">%s</p>'
        icons = ""
        activity = get_docs_since(facility, reference_date)
        programs = set([x.program for x in activity])
        for program in programs:
            if program not in icondic.keys():
                print "Missing", program
                continue
            icondata = icondic[program]
            icons += '<img src="%s" height="25px" alt="%s"/>&nbsp;' % tuple(icondata)
        iconline = iconline % icons
        lines.append(iconline)
    doclist = build_doc_list(facility, reference_date)
    lines.append(doclist)
    popup = ""
    for line in lines:
        popup += "<p>%s</p>\n" % line
    return popup


def build_json_props(facility, reference_date=None):
    popup = facility_to_html(facility, reference_date, add_icons=True)
    props = {
        "name": facility.vfc_name,
        "address": facility.vfc_address,
        "latest activity": get_latest_activity(facility),
        "programs": facility.programs,
        "popupContent": popup,
    }
    return props


def facility_to_point(facility, for_leaflet=True):
    if facility.full_address and not facility.latlong:
        tea_core.latlongify(facility)
    if facility.latlong:
        coords = facility.latlong
    else:
        return None
    if for_leaflet:  # LeafletJS uses reverse of GeoJSON order
        coords = tuple(reversed(coords))
    point = geojson.Point(coords)
    return point


def facility_to_geojson(facility,
                        for_leaflet=True,
                        reference_date=None):
    # 1. put properties into dict
    props = build_json_props(facility, reference_date)
    # 2. get latlong from facility, and if not in facility, from remote service
    point = facility_to_point(facility, for_leaflet)
    feature = geojson.Feature(geometry=point, properties=props)
    return feature


def facilities_to_geojson(facilities, reference_date):
    features = [facility_to_geojson(x, reference_date=reference_date) for x in facilities]
    feature_collection = geojson.FeatureCollection(features)
    return feature_collection


def active_sites_to_geojson(zip_collection, reference_date):
    sites = get_sites_with_activity(zip_collection.facilities, reference_date)
    feature_collection = facilities_to_geojson(sites, reference_date)
    return feature_collection


def facilities_to_html(facilities, reference_date):
    lines = [facility_to_html(facility, reference_date) for facility in facilities]
    line_pattern = '<li class="facility-list-item">%s</li>\n'
    list_pattern = '<ul class="facility-list">\n%s\n</ul>\n'
    list_items = [line_pattern % x for x in lines]
    html_list = list_pattern % ("".join(list_items))
    return html_list


def active_sites_to_html(facilities, reference_date):
    facilities = get_sites_with_activity(facilities, reference_date)
    html_list = facilities_to_html(facilities, reference_date)
    return html_list


def write_updates_html(zip_collection, reference_date, geography="Lake County", today=None, filepath=None):
    if filepath is None:
        filepath = os.path.join(idem_settings.websitedir, "updates.html")
    if today is None:
        today = datetime.date.today()
    facilities = zip_collection.facilities
    list_html = active_sites_to_html(facilities, reference_date)
    forstring = 'for %s for %s' % (geography, today)
    template = '<html><head><title>Updates %s</title></head><body><h1>Updates %s</h1>\n' \
               '<div id="updates">%s</div></body></html>'
    html = template % (forstring, forstring, list_html)
    result = save_or_return(html, filepath)
    return result


def save_or_return(text, filepath=None):
    if filepath:
        open(filepath, "w").write(text)
        return filepath
    else:
        return text


def get_page_patiently(url, session=None, timeout=TIMEOUT):
    """
    :param url: str
    :param session: Session
    :param timeout: int
    :return:
    """
    if session is None:
        session = requests.Session()
    done = False
    tries = 0
    page = ""
    while not done:
        tries += 1
        if tries > 5:
            break
        result = try_to_get_page(url, session, timeout)
        if result is False:
            time.sleep(tries * TIMEOUT)
        else:
            page = result
            done = True
    return page


def try_to_get_page(url, session, timeout=TIMEOUT):
    try:
        handle = session.get(url, timeout=timeout)
    except requests.exceptions.RequestException, e:
        print str(e)
        return False
    else:
        page = handle.text.encode('utf-8', 'ignore')
        return page


def get_doc_url_info(row):
    needle = 'href="(/cs/.+?[^\d](\d+)\.pdf)"'
    linkmatcher = re.search(needle, row)
    if linkmatcher:
        relative_url = linkmatcher.group(1)
        url = idem_settings.ecm_domain + relative_url
        fileid = linkmatcher.group(2)
    else:
        url = ""
        fileid = ""
    return url, fileid


def get_doc_row_data(row):
    url, fileid = get_doc_url_info(row)
    pieces = re.findall('nowrap="nowrap">(.+?)</div>', row)
    if len(pieces) != 5:
        print "Error!"
        return False
    datestring, program, doctype, public, size = pieces
    month, date, year = [int(x) for x in datestring.split("/")]
    rowdata = (url, fileid, month, date, year, program, doctype, size)
    return rowdata


def build_document_from_row(row, facility, crawl_date=None):
    data = get_doc_row_data(row)
    url, fileid, month, date, year, program, doctype, size = data
    file_date = datetime.date(year, month, date)
    newdoc = Document(facility=facility,
                      url=url,
                      id=fileid,
                      file_date=file_date,
                      type=doctype,
                      program=program,
                      crawl_date=crawl_date,
                      row=data,
                      build=False,
                      session=facility.session,
                      size=int(size)
                      )
    return newdoc


def get_reference_date(lookback=7, today=None):
    if today is None:
        today = datetime.date.today()
    delta = datetime.timedelta(lookback)
    reference_date = today - delta
    return reference_date


def write_usable_json(json_obj, filepath=None):
    json_str = geojson.dumps(json_obj)
    json_str = "var features = " + json_str
    result = save_or_return(json_str, filepath)
    return result


def get_json_filepath(date=None):
    if date is None:
        date = datetime.date.today()
    filename = "idem_%s.json" % date.isoformat()
    filepath = os.path.join(idem_settings.maindir, filename)
    return filepath


def save_active_sites_as_json(collection, lookback=7, filepath=None, also_save=True):
    refdate = get_reference_date(lookback)
    print "Reference date: ", refdate.isoformat()
    if filepath is None:
        filepath = get_json_filepath()
    json_obj = active_sites_to_geojson(collection, refdate)
    result = write_usable_json(json_obj, filepath)
    if also_save:
        write_usable_json(json_obj, latest_json_path)
        tea_core.timestamp_directory(idem_settings.websitedir)
    return result


def do_cycle(zips=lakezips):
    cycler = ZipCycler(zips=zips)
    cycler.cycle()


def setup_collection(zips=lakezips):
    collection = ZipCollection(zips=zips, whether_download=False)
    collection.reload_latlongs()
    return collection


def do_cron():
    # first, cycle through VFC for new files
    do_cycle()
    collection = setup_collection()
    save_active_sites_as_json(collection, lookback=7)
    return collection


if __name__ == "__main__":
    do_cron()
