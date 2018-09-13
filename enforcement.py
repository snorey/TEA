import datetime
import geojson
import os
import pickle
import re
import urllib2

import idem_settings
import tea_core

enforcementdir = idem_settings.enforcementdir

# get enforcements from past 90 days
# process enforcements into standardized data objects
# compare items received with existing items
# process into alerts for specified geographies


class EnforcementQuerySession:

    def __init__(self,
                 county="Lake",
                 today=datetime.date.today(),
                 days=90,
                 directory=enforcementdir,
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
        if address is False:
            address = ""
        facility = EnforcementSite(city=city,
                                   county=county,
                                   name=name,
                                   date=date,
                                   full_address=address)
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
        writefile = open(os.path.join(self.directory, "updates_" + self.today.isoformat() + ".txt"), "w")
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

    def latlongify(self):
        if self.full_address:
            tea_core.latlongify(self)
        else:
            pseudo_address = ", ".join([self.name, self.city, "IN", "USA"])  # todo: fix this to use viewport biasing
            result = tea_core.coord_from_address(pseudo_address)
            tea_core.apply_data_to_facility(self, result)


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


def build_popup(doc):
    popup = ""
    if doc.doc_type:
        doctype = doc.doc_type
    else:
        doctype = "Document"
    if doctype == "NOV":
        doctype = "Notice of Violation"
    elif doctype == "AO":
        doctype = "Agreed Order"
    linkline = '<div class="popup-link"><a href="%s" target="blank">%s</a></div>' % (doc.url, doctype)
    popup += linkline + "\n"
    popup += "<p>%s</p>\n" % doc.date
    popup += "<p>%s</p>\n" % doc.facility.name
    popup += "<p>%s</p>\n" % doc.facility.street_address
    return popup


def doc_to_geojson(doc,
                   attempt_latlong=True,
                   for_leaflet=True):
    facility = doc.facility
    # convert doc to geojson Feature:
    # 1. put properties into dict
    props = {
        "name": doc.facility.name,
        "address": doc.facility.full_address,
        "date": doc.date.isoformat(),
        "docType": doc.doc_type,
        "url": doc.url,
        "popupContent": build_popup(doc),
        # "documentContent": doc.content,  # Unicode issues with serialization
    }
    # 2. obtain latlong if not present
    if attempt_latlong and not facility.latlong:
        facility.latlongify()
    if not facility.latlong:
        return False
    coords = facility.latlong
    if for_leaflet:  # LeafletJS uses reverse of GeoJSON order
        coords = tuple(reversed(coords))
    # 3. compose as Point with properties
    point = geojson.Point(coords)
    feature = geojson.Feature(geometry=point, properties=props)
    return feature


def actions_to_geojson(docs, attempt_latlong=True):
    feature_list = []
    for doc in docs:
        # doc to geojson Feature
        feature = doc_to_geojson(doc, attempt_latlong=attempt_latlong)
        if feature is False:
            continue
        # put into list form for subsequent conversion
        feature_list.append(feature)
    # combine into geojson FeatureCollection
    collection = geojson.FeatureCollection(feature_list)
    json = geojson.dumps(collection)
    return json


def write_usable_json(docs, filepath=None):
    if filepath is None:
        filepath = get_json_filepath()
    json = actions_to_geojson(docs)
    json = "var enforcements = " + json
    open(filepath, "w").write(json)
    return filepath


def daily_action(date=datetime.date.today()):
    session = EnforcementQuerySession(today=date)
    session.fetch_all()
    return session


def get_daily_filepath(suffix, date=None):
    filepath = tea_core.get_daily_filepath(suffix, date, enforcementdir)
    return filepath


def get_json_filepath(date=None):
    filepath = get_daily_filepath("json", date=date)
    return filepath


def mock_specific_date(date):
    print date.isoformat()
    session = tea_core.do_patiently(daily_action, date=date)
    return session


def mock_dates(start, end):  # for retroactively creating history
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
    directory = enforcementdir
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


def iso_to_datetime(isodate):
    year = int(isodate[:4])
    month = int(isodate[5:7])
    day = int(isodate[8:10])
    date = datetime.date(year, month, day)
    return date


def get_best_line_for_doc(doc, lines):
    """
    Out of the lines from the "facilitydump.txt" file, find best match for a particular enforcement action.
    :param doc: Document
    :param lines: cleaned and separated lines from the facilitydump.txt file
    :return: str
    """
    if doc.facility.full_address:
        return
    words = doc.name.split(" ")
    matches = [x for x in lines if words[0] in x and doc.city in x]  # assumes NAME in caps and City in titlecase
    print doc.name, doc.city, len(matches)
    if not matches:
        return
    elif len(matches) == 1:
        best_match = matches[0]
    else:
        better_matches = [x for x in matches if x.startswith(doc.name)]
        if better_matches:
            best_match = better_matches[0]
        else:
            best_match = matches[0]
    return best_match


def assign_vfc_data_to_doc(document, line):
    vfc_id, vfc_name, lat, lon, address = line.split("\t")
    fac = document.facility
    fac.vfc_id = vfc_id
    fac.vfc_name = vfc_name
    if lat and lon:
        lat = float(lat)
        lon = float(lon)
        fac.latlong = (lat, lon)
    fac.full_address = address


def pull_vfc_geodata(docs):
    from idem import latlong_filepath
    path = latlong_filepath
    lines = [x.strip() for x in open(path)]
    associations = []
    for d in docs:
        best_match = get_best_line_for_doc(d, lines)
        if not best_match:
            continue
        associations.append((d, best_match))
    for document, line in associations:
        assign_vfc_data_to_doc(document, line)


class DirectoryCycler:

    def __init__(self,
                 docs=None,
                 sessions=None,
                 addr_to_latlong=None,
                 date=None):
        if docs is None:
            docs = set()
        if sessions is None:
            sessions = []
        if date is None:
            date = datetime.date.today()
        self.docs = docs
        self.sessions = sessions
        self.addr_to_latlong = addr_to_latlong  # for storing calls for addresses already looked up
        if self.addr_to_latlong is None:
            self.addr_to_latlong = {}
        self.date = date
        self.directory = enforcementdir

    def process_doc_in_cycle(self, doc):
        doc.file_date = self.date
        site = doc.facility
        addr = site.full_address.lower()  # normalize case
        if not site.latlong:
            if addr in self.addr_to_latlong.keys():
                site.latlong = self.addr_to_latlong[addr]
            else:
                tea_core.latlongify(site)
                self.addr_to_latlong[addr] = site.latlong

    def cycle_through_paths(self, paths):
        for path in paths:
            print path
            filename = os.path.split(path)[-1]
            isodate = filename[:10]
            self.date = iso_to_datetime(isodate)
            session = EnforcementQuerySession(today=self.date, from_page=path)
            # find new docs, and add latlong as needed
            new_docs = session.docs - self.docs
            for doc in new_docs:
                self.process_doc_in_cycle(doc)
            self.sessions.append((isodate, session))
            # add the new docs to the pile
            self.docs |= new_docs
        return self.docs

    def cycle_through_directory(self, county="Lake"):
        def filter_files(this_file):
            pattern = "\d{4}-\d{2}-\d{2}_%s.html" % county
            is_match = re.match(pattern, this_file)
            return is_match
        files = [x for x in os.listdir(self.directory) if filter_files(x)]
        files.sort()
        files = [os.path.join(self.directory, x) for x in files]
        print len(files)
        self.docs = self.cycle_through_paths(files)
        self.docs.sort(key=get_date_of_doc)
        return self.docs

    def get_docs_since(self, lookback=30):
        reference_date = self.date - datetime.timedelta(lookback)
        filtered = []
        for d in self.docs:
            if d.date >= reference_date:
                filtered.append(d)
        filtered.sort(key=get_date_of_doc)
        return filtered


def get_date_of_doc(doc):
    return doc.date


def do_cron():
    daily_action()
    cycler = DirectoryCycler()
    docs = cycler.cycle_through_directory()
    pull_vfc_geodata(docs)
