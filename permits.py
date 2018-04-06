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


