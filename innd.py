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


