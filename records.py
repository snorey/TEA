import datetime
import os
import re
import requests
import time

import idem_settings
import tea_core

# Processing legal notices


class Fetcher:

    def __init__(self):
        self.url = idem_settings.notices_url
        self.session = requests.Session()
        self.pieces = set()
        self.notices = []
        self.pages = ""
        self.page = ""

    def fetch_result_page(self, pageno=1):  # starts from 1
        data = {"FullTextType": "0",
                "Count": "100",
                "PrintNoticeList": "",
                "PageNo": str(pageno),
                "DateRange": "",
                "Category": "-1",
                "Keyword": "",
                "SearchType": "1",
                "Newspaper": "0",
                "State": "IN",
                "StartDate": "",
                "EndDate": ""}
        self.page = self.session.post(self.url, data=data).text.encode("utf-8", "ignore")
        pieces = self.break_page_into_pieces()
        self.pieces = pieces
        return pieces

    def break_page_into_pieces(self, page=""):
        if not page:
            page = self.page
        splitter = '<table width="100%" border="0" cellpadding="0" cellspacing="0">'
        pieces = page.split(splitter)[1:]
        pieces = [x.split("</td>")[0] for x in pieces]
        return pieces

    def fetch_all(self):
        newpieces = self.fetch_result_page()
        self.pieces |= set(newpieces)
        self.pages = self.page
        top_page_number = get_top_page_number(self.page)
        for i in range(1, top_page_number+1):
            time.sleep(1)
            newpieces = self.fetch_result_page(i)
            self.pages += self.page
            self.pieces |= set(newpieces)

    def fetch_until_date(self, until):
        pass

    def save(self, path=""):
        if not path:
            path = self.get_page_path()
        open(path, "w").write(self.pages)
        return path

    def load(self, path):
        self.pages = open(path).read()
        self.pieces = set(self.break_page_into_pieces(self.pages))

    def process_pieces(self, pieces=None):
        if pieces is None:
            pieces = self.pieces
        self.notices = []
        for p in pieces:
            self.validate_and_add_notice(p)

    def validate_and_add_notice(self, piece):
        whether_link = bool(get_first_link_in_text(piece))
        whether_body = bool(get_body_of_notice(piece))
        if not all([whether_link, whether_body]):
            return
        else:
            new_notice = Notice(from_input=piece)
            self.notices.append(new_notice)

    @staticmethod
    def get_page_path():
        today = datetime.date.today()
        filename = "public_notices_%s.html" % today
        path = os.path.join(idem_settings.noticedir, filename)
        return path


def get_first_link_in_text(text):
    pieces = text.split('<a href="', 1)
    if len(pieces) < 2:
        return None
    else:
        link = pieces[1]
        link = link.split('"')[0]
        return link


def get_body_of_notice(text):
    pieces = text.split('<img', 1)
    if len(pieces) < 2:
        return
    else:
        body_chunk = pieces[1]
        body_chunk = body_chunk.split("<br>")[1]
        return body_chunk


class Notice(tea_core.Document):

    def __init__(self, from_input=None, **arguments):
        super(Notice, self).__init__(**arguments)
        self.raw = ""
        self.url = ""
        self.body = ""
        self.title = ""
        self.newspaper = ""
        self.dates = []
        self.rough_address = ""
        self.interesting = False
        self.eventtype = None
        if from_input is not None:
            self.raw = from_input
            self.process_input(from_input)

    def process_input(self, content):
        domain = "http://in.mypublicnotices.com"
        self.content = content
        link = get_first_link_in_text(content)
        self.url = domain + link
        body_chunk = content.split('<img')[1].split("<br>")[1]
        if "<B>" in body_chunk:
            self.title = body_chunk.split("<B>")[1].split("</B>")[0]
            self.body = body_chunk.strip()
        else:
            self.title = content.split('<img')[0]
            self.body = content
        self.get_newspaper_info(content=content)
        worthy_words = ["HEARING", "MEETING"]
        if any([x in self.title for x in worthy_words]):
            self.interesting = True
        if self.interesting is True:
            # do something
            pass

    def get_newspaper_info(self, content):
        if "Appeared in:" in content:
            newspaper_chunk = content.split("Appeared in:")[1].split("</td>")[0]
            self.newspaper = newspaper_chunk.split("<i>")[1].split("</i>")[0]
            formatted_dates = re.findall("\d\d/\d\d/\d\d\d\d", newspaper_chunk)
            for date_string in formatted_dates:
                new_date = datetime.datetime.strptime(date_string, "%m/%d/%Y")
                self.dates.append(new_date)
            # "Appeared in: <b><i>The Times</i></b> on 01/12/2018 and 01/19/2018<"


def get_top_page_number(page):
    def split_me(chunk):
        return chunk.split(")")[0]
    pagenumbers = [split_me(x) for x in page.split('javascript:JumpToResultsPage(')[1:]]
    pagenumbers = [int(x) for x in pagenumbers]
    top_page_number = sorted(pagenumbers)[-1]
    return top_page_number


def do_cron():
    fetcher = Fetcher()
    fetcher.fetch_all()
    fetcher.save()


if __name__ == "__main__":
    do_cron()
