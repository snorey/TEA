# -*- coding: utf-8 -*-

import collections
import datetime
import geojson
import idem_settings
import os
import tea_core
import urllib
import urllib2

permitdir = idem_settings.permitdir
tsv_first_line = "name	URL	PM	number	county	dates	VFC	address	latlong"


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

    def __init__(self, **arguments):
        super(Permit, self).__init__(**arguments)
        self.facility = Facility()
        if "tsv" in arguments.keys():
            tsv = arguments["tsv"]
            self.from_tsv(tsv)
            self.convert_dates()
        if "row" in arguments.keys():
            row = arguments["row"]
            row = row.replace("&amp;", "&")
            name, doc_type, dates, comment, more = tuple(
                [x.split(">", 1)[1].split("</td>")[0] for x in row.split("<td", 5)[1:]])
            self.facility = Facility(name=name)
            self.doc_type = doc_type.split(">")[1].split("[")[0].split("<")[0].strip()
            url = doc_type.split('href="')[1].split('"')[0]
            if url.startswith("/"):
                url = "http://www.in.gov" + url
            self.url = url
            self.more = more
            self.pm = self.extract_info("Project Manager:")
            self.number = self.extract_info("Permit Number:")
            # to do: get program
            if comment == "Yes":
                self.comment_period = dates
                self.convert_dates()
            self.row = row

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
        print line
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


class PermitUpdater:
    directory = permitdir
    main_url = "http://www.in.gov/idem/6395.htm"
    whether_download = True
    logtext = ""
    county = "Lake"

    def __init__(self):
        self.date = datetime.date.today()
        self.current = set()
        self.new = set()
        self.old = set()
        self.urldic = {}

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

    def rebuild_urldic(self):
        for permit in self.current:
            if permit.url:
                self.urldic[permit.url] = permit

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
        newpage = self.check_new_permits()
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
        open(path, "w").write(dump)
        return path

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
        self.logtext = logtext
        return logtext

    def download_new_permits(self, skip_existing=True):
        if not self.whether_download:
            return False
        files = set(os.listdir(permitdir))
        for permit in self.new:
            fileurl = permit.url
            filename = fileurl.split("/")[-1]
            filename = self.date.isoformat() + "_" + permit.county + "_" + permit.facility.name + "_" + filename
            if skip_existing:
                if filename in files:
                    continue
            filepath = os.path.join(self.directory, filename)
            urllib.urlretrieve(fileurl, filepath)
        return True

    def get_open_permits(self):
        open_permits = filter(lambda x: x.is_comment_open(date=self.date), self.current)
        return open_permits

    @staticmethod
    def make_json_path():
        today = datetime.date.today().isoformat()
        extension = ".json"
        name = today + "_permits_" + extension
        return name

    def to_tsv(self, filepath=None):
        tsv = tsv_first_line + "\n"
        for permit in self.current:
            newline = permit.to_tsv()
            tsv += newline + "\n"
        if filepath is None:
            return tsv
        else:
            open(filepath, "w").write(tsv)
            return filepath

    def from_tsv(self, filepath=None):
        if filepath is None:
            filepath = get_tsv_filepath(self.date)
        tsv = open(filepath).read()
        permits = tsv_to_permits(tsv)
        existing_urls = set(self.urldic.keys())
        for p in permits:
            if p.url in existing_urls:
                existing_permit = self.urldic[p.url]
                if not existing_permit.facility.name:
                    existing_permit.facility.name = p.facility.name
                if not existing_permit.facility.full_address:
                    existing_permit.facility.full_address = p.facility.full_address
                if not existing_permit.facility.latlong:
                    existing_permit.facility.latlong = p.facility.latlong
                if not existing_permit.facility.vfc_id:
                    existing_permit.facility.vfc_id = p.facility.vfc_id
            else:
                self.current.add(p)
        self.rebuild_urldic()
        return permits

    def load_vfc(self):
        """
        Load available geographic data based on VFC ID assigned to facility associated with each permit.
        :return: None
        """
        vfcable = [x for x in self.current if x.facility.vfc_id]
        ids = [x.facility.vfc_id for x in vfcable]
        result = ids_to_facilities(ids)  # name, latlong, address
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
        Generate latlong for all permits
        :return: None
        """
        for permit in self.current:
            if permit.facility.latlong:
                continue
            elif permit.facility.full_address:
                permit.facility.latlongify()

    def to_json(self, county="Lake"):
        """
        :param county: str
        :return: geojson object
        """
        documents = self.current
        json = active_permits_to_geojson(documents, county)
        return json

    def save_as_json(self):
        """
        :return: str
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


def daily_permit_check():
    updater = PermitUpdater()
    updater.do_daily_permit_check()
    updater.save_active_permits(county="Lake")
    return updater


# because of the limited automatically available data,
# we'll just generate a skeleton that can be filled in
# manually.
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


def filter_active_permits(documents, county="Lake"):
    # sort by start_date and filter out closed permits
    documents_ = set(documents)
    sortable = [(x.start_date, x) for x in documents_ if x.is_comment_open()]
    sortable.sort()
    documents_filtered = [x[1] for x in sortable]
    if county is not None:
        documents_filtered = [x for x in documents_filtered if x.county.upper() == county.upper()]
    return documents_filtered


def active_permits_to_geojson(documents, county=None):
    # filter out inactive and inapplicable documents
    documents = filter_active_permits(documents, county=county)
    # convert to feature list
    features = []
    for doc in documents:
        new_feature = doc_to_geojson(doc)
        features.append(new_feature)
    collection = geojson.FeatureCollection(features)
    return collection


def build_permit_table(tsv):
    newtable = '<h3>New permit notices</h3>\n<table><tr><th colspan="3">New notices today</th></tr>'
    newtable += '\n<tr><th width="30%">Site</th><th>Document</th><th width="30%">Dates</th></tr>'
    lines = tsv.split("\n")
    lines = filter(lambda x: x.strip(), lines)
    lines = filter(lambda x: "\t" in x, lines)
    for line in lines:
        if line.count("\t") != 6:
            print "Error! %d tabs" % line.count("\t")
        site, doctype, url, pm, permitnum, county, dates = line.split("\t")
        newrow = "\n<tr>%s%s</tr>"
        sitecell = '<td><a href="%s">%s (%s County)</a></td>' % (url, site, county)
        datecell = "<td>%s</td>" % dates
        newrow = newrow % (sitecell, datecell)
        newtable += newrow
    newtable += "</table>"
    return newtable


def ids_to_facilities(ids):
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


def get_latest_tsv():
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
    :param updater: PermitUpdater
    :param path: filepath to be written to
    :type path: str
    :return: None
    """
    json = updater.to_json()
    json_str = geojson.dumps(json)
    json_str = "var permits = " + json_str
    open(path, "w").write(json_str)


def destring_latlong(str_latlong):
    """
    :param str_latlong: latlong tuple after coercion to string
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
    linkline = '<a href="%s" target="blank">%s</a>' % (url, name)
    description = address
    if permit.comment_period:
        period = permit.comment_period
    else:
        period = ""
    for line in [linkline, description, period]:
        popup += "<p>%s</p>\n" % line
    return popup


def do_cron():  # may need to split this into a morning and evening cron
    updater = PermitUpdater()
    updater.do_daily_permit_check()
    inpath = get_latest_tsv()
    updater.from_tsv(inpath)
    updater.load_vfc()
    updater.latlongify()
    outpath = get_tsv_filepath()
    updater.to_tsv(outpath)
    jsonpath = get_json_filepath()
    write_usable_json(updater, jsonpath)


if __name__ == "__main__":
    do_cron()
