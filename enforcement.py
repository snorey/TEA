import datetime
import os
import re
import requests
import time
import urllib2
from idem_settings import enforcementdir

DEFAULT_WAIT = 3
DEFAULT_WAIT_AFTER_ERROR = 30

# get enforcements from past 90 days
# process enforcements into standardized data objects
# compare items received with existing items
# process into alerts for specified geographies


class EnforcementQuerySession:

    def __init__(self):
        pass


class EnforcementDownloadSession:

    def __init__(self):
        pass


class EnforcementDoc:

    def __init__(self):
        pass


class EnforcementSite:

    def __init__(self, **arguments):
        for key, value in arguments.items():
            if not isinstance(value, basestring):
                print "Ignoring value %s for %s" % (str(value), str(key))
                continue
            setattr(self, key, value)


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
            doc_type = actionurl.split("/")[-2]
            filename = company + "_" + city + "_" + doc_type + "_" + basefilename
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
            time.sleep(DEFAULT_WAIT)
            try:
                actionpage = urllib2.urlopen(actionurl, timeout=100).read()
            except urllib2.HTTPError:  # 404
                pass
            except urllib2.URLError, e:  # timed out
                print str(e)
                time.sleep(DEFAULT_WAIT_AFTER_ERROR)
                # attempt retrieval again?
            else:
                open(filepath, "w").write(actionpage)
                updates.append((today.isoformat(), filename))
    if updates:
        tsv = "\n".join(["\t".join(x) for x in updates])
        writefile = open(os.path.join(directory, "updates_" + today.isoformat() + ".txt"), "w")
        with writefile:
            writefile.write(tsv)
    return updates


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


def get_enforcement_address(page):
    clean_page = get_clean_page(page)
    address_finder = "located (.+)"
    city_finder = " in (.+?),"
    address_found = re.search(address_finder, clean_page)
    if not address_found:
        return False
    address_plus = address_found.group(1)
    print address_plus
    if address_plus.startswith("at "):  # sometimes "located", sometimes "located at"
        address_plus = address_plus[3:]
    street_address = address_plus.split(",")[0].split(" in ")[0]
    street_address = repair_ordinals(street_address)
    street_address = street_address.strip()
    city = ""
    city_found = re.search(city_finder, address_plus)
    if city_found:
        city = city_found.group(1)
    else:
        city_finder_2 = ", (.+?), .+? County, Indiana"
        city_found = re.search(city_finder_2, address_plus)
        if city_found:
            city = city_found.group(1)
    city = city.strip()
    address = ", ".join([street_address, city, "IN"])
    address = remove_linebreaks_and_whitespace(address)
    return address
