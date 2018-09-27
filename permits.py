# -*- coding: utf-8 -*-

import collections
import datetime
import geojson
import idem_settings
import operator
import os
import tea_core
import urllib
import urllib2

from tea_core import write_text_to_file

permitdir = idem_settings.permitdir
tsv_first_line = "name	URL	PM	number	county	dates	VFC	address	latlong"
latest_json_path = os.path.join(idem_settings.websitedir, "latest_permits.json")


class Facility(tea_core.Facility):

    def __init__(self, **arguments):
        super(Facility, self).__init__(**arguments)


class Permit(tea_core.Document):
    pm = ""
    comment_period = ""
    county = ""
    type = ""
    url = ""
    row = ""
    more = ""
    number = ""
    end_date = None
    start_date = None
    directory = permitdir

    def __init__(self, tsv="", **arguments):
        super(Permit, self).__init__(**arguments)
        self.facility = Facility()
        if tsv:
            self.from_tsv(tsv)
            self.convert_dates()
        if "row" in arguments.keys():
            row = arguments["row"]
            self.from_row(row)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        if self.url:
            return hash(self.url)
        else:
            return hash(self.data())

    def __str__(self):
        attrs = [x for x in dir(self) if not callable(x)]
        valuepairs = [(x, getattr(self, x)) for x in attrs]
        lines = ["%s: %s" % x for x in valuepairs]
        output = "\n".join(lines)
        return output

    def data(self):
        data = (self.facility.name, self.type, self.url, self.pm, self.number, self.county, self.comment_period)
        data = tuple(map(lambda x: str(x), data))
        return data

    def get_date_string(self, name="start_date"):  # start_date or end_date
        if hasattr(self, name):
            date = getattr(self, name)
            return date.isoformat()
        else:
            return ""

    def extract_info(self, label):
        if label in self.more:
            value = self.more.split(label)[1]
            value = value.split("<")[0].split("\n")[0].strip()
            return value
        else:
            return False

    def download(self):
        filename = self.get_filename()
        filepath = os.path.join(self.directory, filename)
        urllib.urlretrieve(self.url, filepath)
        return filepath

    def is_comment_open(self, date=datetime.date.today()):
        if not hasattr(self, "start_date") or not hasattr(self, "end_date"):
            return False
        if not self.start_date or not self.end_date:
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
            if len(pieces) == 1:
                pieces = datestring.strip().split(" ")
                pieces = [pieces[0], pieces[-1]]
            start, stop = pieces
            self.start_date = self.convert_single_date(start)
            self.end_date = self.convert_single_date(stop)

    @staticmethod
    def convert_single_date(usdate):
        month, day, year = usdate.split("/")
        if len(month) < 2:
            month = month.zfill(2)
        if len(day) < 2:
            day = day.zfill(2)
        if len(year) != 4:
            year = str(datetime.date.today().year)
        usdate_cleaned = "%s/%s/%s" % (month, day, year)
        date = datetime.datetime.strptime(usdate_cleaned, "%m/%d/%Y").date()
        return date

    def to_tsv(self):
        """name	URL	PM	number	county	dates	VFC	address	latlong"""
        name = self.facility.name
        url = self.url
        pm = self.pm
        number = self.number
        county = self.county
        dates = self.comment_period
        vfc = self.facility.vfc_id
        address = self.facility.full_address
        latlong = self.facility.latlong
        attrs = [name, url, pm, number, county, dates, vfc, address, latlong]

        def rectify(attribute):
            if bool(attribute) is False:
                return ""
            else:
                return str(attribute)
        attrs = [rectify(x) for x in attrs]
        tsv = "\t".join(attrs)
        return tsv

    def from_tsv(self, line):
        name, url, pm, number, county, dates, vfc, address, latlong = line.split("\t")
        self.facility.name = name
        self.url = url
        self.pm = pm
        self.number = number
        self.county = county
        self.comment_period = dates
        self.facility.vfc_id = vfc
        self.facility.full_address = address
        if latlong:
            self.facility.latlong = destring_latlong(latlong)

    def from_row(self, row):
        row = row.replace("&amp;", "&")
        cells = self.get_cells_from_row(row)
        name, doc_cell, dates, comment, more = cells
        self.facility = Facility(name=name)
        self.doc_type, self.url = self.get_data_from_doc_cell(doc_cell)
        self.more = more
        self.pm = self.extract_info("Project Manager:")
        self.number = self.extract_info("Permit Number:")
        self.program = infer_program_from_url(self.url)
        if comment == "Yes":
            self.comment_period = dates
            self.convert_dates()
        self.row = row

    @staticmethod
    def get_cells_from_row(row):
        chunks = row.split("<td", 5)
        chunks = chunks[1:]
        cells = []
        for chunk in chunks:
            cell = chunk.split(">", 1)[1].split("</td>")[0]
            cells.append(cell)
        return cells

    @staticmethod
    def get_data_from_doc_cell(doc_cell):
        doc_type = doc_cell.split(">")[1].split("[")[0].split("<")[0]
        doc_type = doc_type.strip()
        url = doc_cell.split('href="')[1].split('"')[0]
        if url.startswith("/"):
            url = "http://www.in.gov" + url
        return doc_type, url

    def get_filename(self):
        fileurl = self.url
        original_filename = fileurl.split("/")[-1]
        facility_name = self.facility.name[:50]  # occasionally these may be e.g. a verbose program description
        prefix = self.date.isoformat() + "_" + self.county + "_" + facility_name
        filename = prefix + "_" + original_filename
        return filename


def infer_program_from_url(url):
    url = url.lower()
    if "npdes" in url:
        return "Wastewater"
    if "air" in url:
        return "Air"
    if "dw" in url or "pws" in url:
        return "Drinking water"
    else:
        return ""


class PermitUpdater:
    directory = permitdir
    main_url = "http://www.in.gov/idem/6395.htm"
    whether_download = True
    logtext = ""
    county = "Lake"
    skip_existing = True

    def __init__(self):
        self.date = datetime.date.today()
        self.current = set()
        self.new = set()
        self.old = set()
        self.active = []
        self.urldic = {}
        self.files = self.update_file_list()

    @property
    def page_path(self):
        today = datetime.date.today().isoformat()
        filename = self.main_url.split("/")[-1]
        filename = today + "_" + filename
        path = os.path.join(self.directory, filename)
        return path

    @property
    def active_and_relevant(self):
        """
        Return the permits that are of actual interest: comment is open and they are in the right county.
        :return: list of permits
        """
        active = []
        for permit in self.current:
            if permit.is_comment_open():
                if self.is_relevant(permit):
                    active.append(permit)
        return active

    def is_relevant(self, permit):
        result = is_relevant(permit, self.county)
        return result

    def retrieve_permit_page(self):
        """
        Download and return today's permit page.
        :return: HTML as str
        """
        page = urllib2.urlopen(self.main_url).read()
        write_text_to_file(page, self.page_path)
        return page

    def compare_permits(self, newpage, oldpage):
        """
        Compare two versions of the IDEM webpage and return a set of added permits and set of removed permits.
        :param newpage: HTML
        :param oldpage: HTML
        :return: tuple of sets of Permit objects
        """
        newpermits = self.get_permits_from_page(newpage)
        oldpermits = self.get_permits_from_page(oldpage)
        added = newpermits - oldpermits
        subtracted = oldpermits - newpermits
        return added, subtracted

    @staticmethod
    def get_permits_from_section(countychunk):
        permits = set()
        county, countyrows = tuple(countychunk.split("</th>", 1))
        if countyrows.count("</tr>") < 2:
            return set()
        rows_split = countyrows.split("</tr>")[1:-1]
        for row in rows_split:
            if row.count("<td") < 5:
                continue
            newpermit = Permit(row=row)
            newpermit.county = county
            permits.add(newpermit)
        return permits

    def get_permits_from_page(self, page):
        permits = set()
        countychunks = page.split('<th class="section" colspan="5">')[1:]
        for c in countychunks:
            countypermits = self.get_permits_from_section(c)
            permits |= countypermits
        return permits

    def rebuild_urldic(self):
        for permit in self.current:
            if permit.url:
                self.urldic[permit.url] = permit

    def do_daily_permit_check(self, date=None):
        if date is None:
            date = datetime.date.today()
        files = os.listdir(self.directory)
        files.sort()
        files = filter(lambda x: x.endswith(".htm"), files)
        if not files:
            oldpage = ""
        else:
            latest = files[-1]
            today = date.isoformat()
            if today in latest:  # avoid tripping over self
                if len(files) > 1:
                    latest = files[-2]
                    print latest
            latestpath = os.path.join(self.directory, latest)
            oldpage = open(latestpath).read()
        newpage = self.retrieve_permit_page()
        self.current = self.get_permits_from_page(newpage)
        self.new, self.old = self.compare_permits(newpage, oldpage)
        self.rebuild_urldic()
        logtext = self.log_permit_updates()
        self.download_new_permits()
        return logtext

    def save_active_permits(self, county="Lake", path=""):
        active_permits = self.get_open_permits()
        json = active_permits_to_geojson(active_permits, county=county)
        if not path:
            path = self.make_json_path()
        dump = geojson.dumps(json, sort_keys=True)
        write_text_to_file(path, dump)
        return path

    def generate_log_text(self):
        logtext = ""
        logtext += "*** New notices today***\n\n"
        if self.new:
            for permit in self.new:
                logtext += permit.to_tsv() + "\n"
        else:
            logtext += "No new notices.\n"
        logtext += "\n\n*** Notices removed today***\n\n"
        if self.old:
            for permit in self.old:
                logtext += permit.to_tsv() + "\n"
        else:
            logtext += "No notices removed today.\n"
        return logtext

    def log_permit_updates(self):
        if not self.new and not self.old:
            return ""
        logfilename = "updates_%s.txt" % self.date.isoformat()
        logpath = os.path.join(self.directory, logfilename)
        logtext = self.generate_log_text()
        write_text_to_file(logtext, logpath)
        self.logtext = logtext
        return logtext

    def update_file_list(self):
        files = os.listdir(self.directory)
        return set(files)

    def download_new_permits(self):
        if not self.whether_download:
            return False
        self.files = set(os.listdir(permitdir))
        for permit in self.new:
            self.download_permit(permit)
        return True

    def download_permit(self, permit):
        filename = permit.get_filename()
        if self.skip_existing:
            if filename in self.files:
                return
        filepath = os.path.join(self.directory, filename)
        tea_core.retrieve_patiently(permit.url, filepath)
        self.files.add(filename)

    def get_open_permits(self):
        open_permits = filter(lambda x: x.is_comment_open(date=self.date), self.current)
        return open_permits

    @staticmethod
    def make_json_path():
        today = datetime.date.today().isoformat()
        extension = ".json"
        name = today + "_permits_" + extension
        return name

    def process_permit_to_tsv(self, permit):
        newline = ""
        if self.is_relevant(permit):
            newline = permit.to_tsv()
            newline += "\n"
        return newline

    def to_tsv(self):
        tsv = tsv_first_line + "\n"
        for permit in self.active_and_relevant:
            tsv += self.process_permit_to_tsv(permit)
        return tsv

    def save_to_tsv(self, filepath=None):
        if filepath is None:
            filepath = get_tsv_filepath(self.date)
        tsv = self.to_tsv()
        write_text_to_file(tsv, filepath)
        return filepath

    @staticmethod
    def merge_permits(existing_permit, new_permit):
        for attribute in ["name", "full_address", "latlong", "vfc_id"]:
            old_value = getattr(existing_permit.facility, attribute)
            if not old_value:
                new_value = getattr(new_permit.facility, attribute)
                if new_value:
                    setattr(existing_permit.facility, attribute, new_value)

    def process_added_permit(self, permit):
        if permit.url in self.urldic.keys():
            existing_permit = self.urldic[permit.url]
            self.merge_permits(existing_permit, permit)
        else:
            self.current.add(permit)
            self.urldic[permit.url] = permit

    @staticmethod
    def clean_tsv(tsv):  # repair LibreOffice gunk
        tsv = tsv.replace("+AC0", "")
        return tsv

    def from_tsv(self, filepath=None):
        if filepath is None:
            filepath = get_tsv_filepath(self.date)
        tsv = open(filepath).read()
        tsv = self.clean_tsv(tsv)
        permits = tsv_to_permits(tsv)
        for permit in permits:
            self.process_added_permit(permit)
        return permits

    def load_vfc(self):
        """
        Load available geographic data based on VFC ID assigned to facility associated with each permit.
        """
        vfcable = [x for x in self.current if x.facility.vfc_id]
        ids = [x.facility.vfc_id for x in vfcable]
        result = ids_to_facility_data(ids)  # name, latlong, address
        for v in vfcable:
            facility_data = result[v.facility.vfc_id]
            if facility_data:
                name, latlong, address = facility_data
                if not v.facility.name:  # names given in public notices are often more accurate than VFC names
                    v.facility.name = name
                v.facility.latlong = latlong
                v.facility.full_address = address

    def latlongify(self):
        """
        Generate latlong for all permits.
        """
        for permit in self.current:
            if permit.facility.latlong:
                continue
            elif permit.facility.full_address:
                permit.facility.latlongify()

    def to_json(self):
        documents = [x for x in self.current if self.is_relevant(x)]
        json = active_permits_to_geojson(documents)
        return json

    def save_as_json(self):
        """
        Saves JSON representation of permits to the standard filepath.
        :return: Path to newly-saved JSON file
        """
        jsonpath = get_json_filepath(self.date)
        write_usable_json(self, jsonpath)
        return jsonpath


def tsv_to_permits(tsv):
    lines = [x for x in tsv.split("\n") if x.strip()]
    if not lines:
        return set()
    if lines[0][:5] == tsv_first_line[:5]:
        lines = lines[1:]
    permits = [Permit(tsv=x) for x in lines]
    permits = set(permits)
    return permits


def doc_to_geojson(permit,
                   for_leaflet=True):
    facility = permit.facility
    # convert doc to geojson Feature:
    # 1. put properties into dict
    props = {
        "name": permit.facility.name,
        "address": permit.facility.full_address,
        "date": permit.date.isoformat(),
        "docType": permit.doc_type,
        "manager": permit.pm,
        "program": permit.program,
        "url": permit.url,
        "popupContent": build_popup(permit),
        "start": permit.get_date_string("start_date"),
        "end": permit.get_date_string("end_date"),
    }
    # 2. obtain latlong if not present.
    # If no address, just leave blank to fill in.
    if facility.full_address and not facility.latlong:
        facility.latlongify()
    if not facility.latlong:
        return None
    coords = facility.latlong
    if for_leaflet:  # LeafletJS uses reverse of GeoJSON order
        coords = tuple(reversed(coords))
    # 3. compose as Point with properties
    point = geojson.Point(coords)
    feature = geojson.Feature(geometry=point, properties=props)
    return feature


def is_relevant(permit, county):
    if county.upper() == permit.county.upper():
        return True
    elif permit.county.upper().startswith("MULTI"):
        return True
    else:
        return False


def filter_active_permits(documents, county="Lake"):
    """
    Sort by start_date and filter out closed permits.
    :param documents: list of documents to be filtered
    :param county: county to filter by (may be None)
    :return: list of active permits in selected county
    """
    unique_documents = set(documents)
    sortable = [x for x in unique_documents if x.is_comment_open()]
    sortable.sort(key=operator.attrgetter("start_date"))
    documents_filtered = sorted(sortable)
    if county is not None:
        documents_filtered = [x for x in documents_filtered if is_relevant(x, county)]
    return documents_filtered


def permits_to_geojson(documents):
    features = []
    for doc in documents:
        new_feature = doc_to_geojson(doc)
        if new_feature is not None:
            features.append(new_feature)
    collection = geojson.FeatureCollection(features)
    return collection


def active_permits_to_geojson(documents, county=None):
    # filter out inactive and inapplicable documents
    documents = filter_active_permits(documents, county=county)
    # convert to feature list
    collection = permits_to_geojson(documents)
    return collection


def ids_to_facility_data(ids):
    iddic = collections.defaultdict(tuple)
    from idem import get_location_data
    data = get_location_data()
    for datum in data:
        facility_id, name, latlong, address = datum
        iddic[facility_id] = (name, latlong, address)
    output = {}
    for facility_id in ids:
        output[facility_id] = iddic[facility_id]
    return output


def get_daily_filepath(suffix, date=None):
    filepath = tea_core.get_daily_filepath(suffix, date, permitdir)
    return filepath


def get_tsv_filepath(date=None):
    filepath = get_daily_filepath("tsv", date=date)
    return filepath


def get_json_filepath(date=None):
    filepath = get_daily_filepath("json", date=date)
    return filepath


def get_latest_tsv_path():
    files_in_directory = os.listdir(permitdir)
    tsvfiles = [x for x in files_in_directory if x.startswith("permits_") and x.endswith(".tsv")]
    if not tsvfiles:
        return None
    else:
        tsvfiles.sort()
        latestfile = tsvfiles[-1]
        filepath = os.path.join(permitdir, latestfile)
        return filepath


def write_usable_json(updater, path):
    """
    Given an updater and a filepath, writes a viable JS file to the filepath.
    :param updater: PermitUpdater
    :param path: filepath to be written to
    :type path: str
    :return: None
    """
    json = updater.to_json()
    json_str = geojson.dumps(json)
    json_str = "var permits = " + json_str + ";"
    write_text_to_file(json_str, path)


def destring_latlong(str_latlong):
    """
    :param str_latlong: string-coerced latlong tuple
    :type str_latlong: str
    :return: tuple
    """
    if ", " not in str_latlong:
        return str_latlong
    str_latlong = str_latlong.strip()[1:-1]  # trim parentheses
    pieces = str_latlong.split(", ")
    lat, lon = [float(x) for x in pieces]
    return lat, lon


def build_popup(permit):
    """
    :param permit: the permit object for which a popup is needed
    :type permit: Permit
    :return: str
    """
    popup = ""
    url = permit.url
    name = permit.facility.name
    address = permit.facility.full_address
    nameline = name
    if permit.program:
        nameline += " (%s)" % permit.program
    description = address
    if permit.comment_period:
        period = "Comment period: " + permit.comment_period
    else:
        period = ""
    anchor = "Read document"
    if permit.doc_type:
        anchor = permit.doc_type
    docline = '<a href="%s" target="blank">%s</a>' % (url, anchor)
    for line in [nameline, description, docline, period]:
        popup += "<p>%s</p>\n" % line
    return popup


def daily_permit_check():
    updater = PermitUpdater()
    updater.do_daily_permit_check()
    updater.save_active_permits(county="Lake")
    return updater


def do_cron():  # may need to split this into a morning and evening cron
    updater = PermitUpdater()
    updater.do_daily_permit_check()
    tsv_path_in = get_latest_tsv_path()
    updater.from_tsv(tsv_path_in)
    updater.load_vfc()
    updater.latlongify()
    tsv_path_out = get_tsv_filepath()
    updater.save_to_tsv(tsv_path_out)
    jsonpath = get_json_filepath()
    write_usable_json(updater, jsonpath)
    write_usable_json(updater, latest_json_path)
    return updater


if __name__ == "__main__":
    do_cron()
