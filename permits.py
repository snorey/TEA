# -*- coding: utf-8 -*-

import datetime
import geojson
import idem_settings
import os
import tea_core
import urllib
import urllib2

permitdir = idem_settings.permitdir


class Facility(tea_core.Facility):

    def __init__(self, **arguments):
        super(Facility, self).__init__(**arguments)


class Permit(tea_core.Document):
    pm = ""
    facility = Facility()
    comment_period = ""
    county = ""
    type = ""
    url = ""
    row = ""
    more = ""
    number = ""

    def __init__(self, **arguments):
        super(Permit, self).__init__(**arguments)
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
        return hash(self.data())

    def __str__(self):
        return "\n".join(["%s: %s" % (x, getattr(self, x)) for x in dir(self) if not callable(x)])

    def data(self):
        data = (self.facility.name, self.type, self.url, self.pm, self.number, self.county, self.comment_period)
        data = tuple(map(lambda x: str(x), data))
        return data

    def get_date_string(self, name="start_date"): # start_date or end_date
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
        date = datetime.datetime.strptime(usdate, "%m/%d/%Y").date()
        return date


class PermitUpdater:
    directory = permitdir
    main_url = "http://www.in.gov/idem/6395.htm"
    whether_download = True
    current = set()
    new = set()
    old = set()

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

    def do_daily_permit_check(self):
        files = os.listdir(self.directory)
        files.sort()
        files = filter(lambda x: x.endswith(".htm"), files)
        if not files:
            oldpage = ""
        else:
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
            filename = self.date.isoformat() + "_" + permit.county + "_" + permit.facility.name + "_" + filename
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


# because of the limited automatically available data,
# we'll just generate a skeleton that can be filled in
# manually.
def doc_to_geojson(permit,
                   for_leaflet=True):
    facility = permit.facility
    # convert doc to geojson Feature:
    # 1. put properties into dict
    props = {
        "name": permit.facility.name, # to do: convert to proper Facility() objects
        "address": permit.facility.full_address,
        "date": permit.date.isoformat(),
        "docType": permit.doc_type,
        "manager": permit.pm,
        "program": permit.program,
        "url": permit.url,
        "popupContent": "",
        "start": permit.get_date_string("start_date"),
        "end": permit.get_date_string("end_date"),
    }
    # 2. obtain latlong if not present.
    # If no address, just leave blank to fill in.
    if facility.full_address and not facility.latlong:
        tea_core.latlongify(facility)
    coords = facility.latlong
    if for_leaflet:  # LeafletJS uses reverse of GeoJSON order
        coords = tuple(reversed(coords))
    # 3. compose as Point with properties
    point = geojson.Point(coords)
    feature = geojson.Feature(geometry=point, properties=props)
    return feature


def geojson_to_doc(json):
    pass


def active_permits_to_geojson(documents):
    features = []
    for doc in documents:
        if not doc.is_comment_open():
            continue
        new_feature = doc_to_geojson(doc)
        features.append(new_feature)
    collection = geojson.FeatureCollection(features)
    return collection


def build_permit_table(tsv):
		newtable = '<h3>New permit notices</h3>\n<table><tr><th colspan="3">New notices today</th></tr>'
		newtable += '\n<tr><th width="30%">Site</th><th>Document</th><th width="30%">Dates</th></tr>'
		lines = tsv.split("\n")
		lines = filter(lambda x:x.strip(),lines)
		lines = filter(lambda x: "\t" in x,lines)
		for line in lines:
			if line.count("\t") != 6:
				print "Error! %d tabs" % line.count("\t")
			site,type,url,pm,permitnum,county,dates = line.split("\t")
			newrow = "\n<tr>%s%s</tr>"
			sitecell = '<td><a href="%s">%s (%s County)</a></td>' % (url, site, county)
			datecell = "<td>%s</td>" % dates
			newrow = newrow % (sitecell, datecell)
			newtable += newrow
		newtable += "</table>"
		return newtable
