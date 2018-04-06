import datetime
import geojson
import os
import pickle
import re
import requests
import time
import urllib2

import idem_settings
import tea_core

# get enforcements from past 90 days
# process enforcements into standardized data objects
# compare items received with existing items
# process into alerts for specified geographies


class EnforcementQuerySession:

    def __init__(self,
                 county="Lake",
                 today=datetime.date.today(),
                 days=90,
                 directory=idem_settings.enforcementdir,
                 from_page="",
                 verbose=False):
        self.county = county
        self.days = days
        self.directory = directory
        self.page = ""
        self.rows = []
        self.docs = set()
        self.today = today
        self.verbose = verbose
        self.url = self.build_url()
        self.filepath = self.build_filepath()
        self.log = True
        self.updates = []
        self.old_docs = set()
        self.sites = set()
        if from_page:
            self.page = open(from_page).read()
            self.page2docs()

    def build_filepath(self):
        filename = self.today.isoformat() + "_" + self.county
        filename += ".html"
        filepath = os.path.join(self.directory, filename)
        return filepath

    def build_url(self):
        today = self.today
        delta = datetime.timedelta(self.days)
        start = today - delta
        startday = start.strftime("%d")
        startmonth = start.strftime("%b")
        startyear = start.strftime("%Y")
        nowday = today.strftime("%d")
        nowmonth = today.strftime("%b")
        nowyear = today.strftime("%Y")
        url = "http://www.in.gov/apps/idem/oe/idem_oe_order?"
        url += "company_name=&case_number=&old_case_number=&county=" + self.county
        url += "&media=All&type=0&start_month=" + startmonth + "&start_day=" + startday
        url += "&start_year=" + startyear + "&end_month=" + nowmonth + "&end_day=" + nowday
        url += "&end_year=" + nowyear + "&page=T&action=Search"
        return url

    def row2obj(self, row):
        actionurl = ""
        basefilename = ""
        doc_type = ""
        city = row.split('<TD>&nbsp;<font size="-1">')[2].split("<")[0]
        company = row.split('<font size="-1">', 1)[1].split("<")[0]
        if "href" in row:
            actionurl = row.split('<a href="')[1].split('"')[0]
            basefilename = actionurl.split("/")[-1]
            doc_type = actionurl.split("/")[-2]
        # define filename even for docs with no file, to avoid hashing problems
        filename = "_".join([company, city, doc_type, basefilename])
        path = os.path.join(self.directory, filename)
        doc = EnforcementDoc(url=actionurl,
                             crawl_date=self.today,
                             filename=filename,
                             path=path,
                             doc_type=doc_type,
                             city=city,
                             name=company)
        return doc

    def page2rows(self):
        rows = self.page.split("<TR>")[1:]
        rows = [x for x in rows if '<TD ALIGN="CENTER"' in x]
        self.rows = rows
        return rows

    def rows2objects(self):
        output = set()
        for row in self.rows:
            doc = self.row2obj(row)
            output.add(doc)
        self.docs = output
        return self.docs
    
    def page2docs(self):
        self.page2rows()
        self.rows2objects()
        self.docs2sites()

    def doc2site(self, document):
        if not document.path:
            return False
        if not document.content:
            document.get_content()
        city = document.city
        county = self.county
        name = document.name
        date = self.today
        address = get_enforcement_address(document)
        facility = EnforcementSite(city=city, county=county, name=name, date=date, address=address)
        facility.downloaded_docs.add(document)
        document.facility = facility
        return facility

    @property
    def sitefinder(self):
        finder = dict([(x.identity, x) for x in self.sites])
        return finder

    def docs2sites(self):  # given docs, generate list of facilities involved, then check for relevant stored data
        for doc in self.docs:
            site = self.doc2site(doc)
            if site in self.sites:
                existing_site = self.sitefinder[site.identity]
                existing_site.downloaded_docs.add(doc)
            else:
                self.sites.add(site)

    def get_updates(self, comparator=""):
        # if no comparator supplied, find most recent download
        if not comparator:
            comparator = tea_core.get_previous_file_in_directory(self.directory,
                                                                 reference_date=self.today.isoformat())
        if self.verbose:
                print comparator
        # if no prior download, then everything is new
        if not comparator:
            old_docs = set()
        # if comparator available, load comparator docs
        else:
            old_docs = EnforcementQuerySession(from_page=comparator).docs
        # identify any docs absent from comparator
        # note that this also distinguishes entries that previously had no link,
        # because the object hash (based on filename) will differ
        self.old_docs = old_docs
        if self.verbose:
            print len(self.docs), len(old_docs)
        new_docs = self.docs - old_docs
        # return updates
        self.updates = new_docs
        return new_docs

    def log_updates(self):
        if not self.log:
            return
        tsv = "\n".join([x.to_tsv() for x in self.updates])
        writefile = open(os.path.join(self.directory, "updates_" + today.isoformat() + ".txt"), "w")
        with writefile:
            writefile.write(tsv)

    def download_files(self):
        already = set(os.listdir(self.directory))
        docs_to_download = self.docs - already
        for doc in docs_to_download:
            if self.verbose:
                print doc.filename
            if not doc.url:
                if self.verbose:
                    print "Bypassing, no URL"
                continue
            filepath = os.path.join(self.directory, doc.filename)
            if self.verbose:
                print filepath
            doc.retrieve_patiently(filepath)

    def fetch_all(self):
        url = self.build_url()
        filepath = self.build_filepath()
        page = urllib2.urlopen(url, timeout=tea_core.TIMEOUT).read()
        self.page = page
        open(filepath, "w").write(page)
        self.page2rows()
        self.rows2objects()
        self.download_files()
        self.docs2sites()
        updates = self.get_updates()
        return updates


class EnforcementDoc(tea_core.Document):

    def __init__(self, **arguments):
        super(EnforcementDoc, self).__init__(**arguments)
        pass

    def get_content(self):  # because these are simple HTML files
        try:
            content = open(self.path).read()
        except IOError:
            content = ""
        self.content = content


class EnforcementSite(tea_core.Facility):

    def __init__(self, **arguments):
        super(EnforcementSite, self).__init__(**arguments)

    def set_directory(self, directory=""):
        pass

    def get_docs(self):
        return set()

    def get_downloaded_docs(self):
        return set()


def remove_comments(html):
    clean_html = re.sub("<!--[\s\S]+?-->", "", html)
    return clean_html


def get_paragraphs(page):
    para_getter = "<p [\s\S]+?>([\s\S]+?)</p>"
    page = remove_comments(page)
    paras = re.findall(para_getter, page)
    return paras


def remove_tags(html):
    clean_text = re.sub("<.+?>", " ", html)
    return clean_text


def remove_linebreaks_and_whitespace(text):
    clean_text = re.sub("\s+", " ", text)
    return clean_text


def clean_paragraph(paragraph):
    new_para = remove_linebreaks_and_whitespace(paragraph)
    new_para = remove_tags(new_para)
    return new_para


def get_clean_page(page):
    paras = get_paragraphs(page)
    paras = [clean_paragraph(x) for x in paras]  # clean up internal linebreaks
    clean_page = "\n".join(paras)
    return clean_page


def repair_ordinals(text):
    matcher = "(\d) (th|rd|d|nd|d) "
    repaired = re.sub(matcher, "\\1\\2 ", text)
    return repaired


def get_enforcement_address(doc):
    if not doc.content:
        doc.get_content()
    page = doc.content
    if not page:
        return False
    clean_page = get_clean_page(page)
    address_finder = "located (.+)"
    city_finder = " in (.+?),"
    address_found = re.search(address_finder, clean_page)
    if not address_found:
        return False
    address_plus = address_found.group(1)
    if address_plus.startswith("at "):  # sometimes "located", sometimes "located at"
        address_plus = address_plus[3:]
    street_address = address_plus.split(",")[0].split(" in ")[0]
    street_address = repair_ordinals(street_address)
    street_address = street_address.strip()
    city = ""
    if doc.city:
        city = doc.city
    else:
        city_found = re.search(city_finder, address_plus)
        if city_found:
            city = city_found.group(1)
        else:
            city_finder_2 = ", (.+?), .+? County, Indiana"
            city_found = re.search(city_finder_2, address_plus)
            if city_found:
                city = city_found.group(1)
    city = city.strip()
    address_pieces = [street_address, city, "IN"]
    address = ", ".join(address_pieces)
    address = remove_linebreaks_and_whitespace(address)
    return address


def latlongify(facility):
    address = facility.full_address
    latitude, longitude, google_address = tea_core.coord_from_address(address)
    facility.full_address = google_address
    lat = float(latitude)
    lon = float(longitude)
    facility.latlong = (lat, lon)


def actions_to_geojson(docs):
    feature_list = []
    for doc in docs:
        # doc to geojson Feature
        feature = doc_to_geojson(doc)
        # put into list form for subsequent conversion
        feature_list.append(feature)
    # combine into geojson FeatureCollection
    collection = geojson.FeatureCollection(feature_list)
    json = geojson.dumps(collection)
    return json


def compose_popup(doc):
    popup = ""
    linkline = '<div class="popup-link"><a href="%s">%s</a></div>' % (doc.url, doc.doc_type)
    popup += linkline + "\n"
    popup += "<p>%s</p>\n" % doc.date
    popup += "<p>%s</p>\n" % doc.facility.name
    popup += "<p>%s</p>\n" % doc.facility.street_address
    return popup  # to do


def doc_to_geojson(doc):
    facility = doc.facility
    # convert doc to geojson Feature:
    # 1. put properties into dict
    props = {
        "name": doc.facility.name,
        "address": doc.facility.address,
        "date": doc.date,
        "docType": doc.doc_type,
        "url": doc.url,
        "popupContent": compose_popup(doc),
        "documentContent": doc.content,
    }
    # 2. obtain latlong if not present
    if not facility.latlong:
        latlongify(facility)
    coords = facility.latlong
    # 3. compose as Point with properties
    point = geojson.Point(coords)
    feature = geojson.Feature(geometry=point, properties=props)
    return feature


def daily_action(date=datetime.date.today()):
    session = EnforcementQuerySession(today=date)
    session.fetch_all()
    return session


def mock_specific_date(date):
    print date.isoformat()
    session = tea_core.do_patiently(daily_action, date=date)
    return session


def mock_dates(start, end):
    delta = datetime.timedelta(1)
    date = start
    sessions = []
    while date <= end:
        session = mock_specific_date(date)
        item = (date.isoformat(), session)
        sessions.append(item)
        date += delta
    return sessions


def make_pickle_path(filename="saved.pickle"):
    directory = idem_settings.enforcementdir
    path = os.path.join(directory, filename)
    return path


def save_docs(docs, path=make_pickle_path()):
    savefile = open(path, "w")
    pickle.dump(docs, savefile)
    return path


def load_docs(path=make_pickle_path()):
    loadfile = open(path)
    docs = pickle.load(loadfile)
    return docs


def iso2datetime(isodate):
    year = int(isodate[:4])
    month = int(isodate[5:7])
    day = int(isodate[8:10])
    date = datetime.date(year, month, day)
    return date


def cycle_through_directory(directory=idem_settings.enforcementdir, county="Lake"):
    def filter_files(this_file):
        pattern = "\d{4}-\d{2}-\d{2}_%s.html" % county
        is_match = re.match(pattern, this_file)
        return is_match
    files = [os.path.join(directory, x) for x in os.listdir(directory)]
    files = [x for x in files if filter_files(x)]
    docs = set()
    addr_to_latlong = {}  # for storing calls for addresses already looked up
    for f in files:
        filename = os.path.split(f)[-1]
        isodate = filename[:10]
        date = iso2datetime(isodate)
        session = EnforcementQuerySession(today=date, from_page=filename)
        # find new docs, and add latlong as needed
        new_docs = docs - session.docs
        for doc in new_docs:
            doc.file_date = date
            site = doc.facility
            addr = site.address.lower()  # normalize case
            if not site.latlong:
                if addr in addr_to_latlong.keys():
                    site.latlong = addr_to_latlong[addr]
                else:
                    latlongify(site)
                    addr_to_latlong[addr] = site.latlong
        # add the new docs to the pile
        docs |= new_docs
    return docs
