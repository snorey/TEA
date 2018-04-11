import datetime
import idem_settings
import re
import requests
import tea_core
import time


class Fetcher:

    def __init__(self):
        self.url = "http://in.mypublicnotices.com/PublicNotice.asp?Page=SEARCHRESULTS"
        self.session = requests.Session()
        self.pieces= set()
        self.notices = []

    def fetch_result_page(self,pageno=1): #starts from 1
        url = self.url
        data = {"FullTextType": "0",
                 "Count": "100",
                 "PrintNoticeList": "",
                 "PageNo":str(pageno),
                 "DateRange":"",
                 "Category":"-1",
                 "Keyword":"",
                 "SearchType":"1",
                 "Newspaper":"0",
                 "State":"IN",
                 "StartDate":"",
                 "EndDate":""}
        self.page = self.session.post(self.url, data=data).text.encode("utf-8","ignore")
        pieces = self.break_page_into_pieces()
        return pieces

    def break_page_into_pieces(self,page=""):
        if not page:
            page = self.page
        splitter = '"SearchResultsHeading"'
        pieces = page.split(splitter)[1:]
        return pieces

    def fetch_all(self):
        newpieces = self.fetch_result_page()
        self.pieces |= set(newpieces)
        self.pages = self.page
        top_page_number = get_top_page_number(self.page)
        for i in range(1,top_page_number+1):
            time.sleep(1)
            newpieces = self.fetch_result_page(i)
            self.pages += self.page
            self.pieces |= set(newpieces)

    def fetch_until_date(self,until):
        pass

    def save(self, path=""):
        if not path:
            path = self.get_page_path()
        open(path,"w").write(self.pages)
        return path

    def load(self,path):
        self.pages = open(path).read()
        self.pieces = set(self.break_page_into_pieces(self.pages))

    def process_pieces(self,pieces=set()):
        if not pieces:
            pieces = self.pieces
        self.notices = []
        for p in pieces:
            self.notices.append(Notice(input=p))

    def get_page_path(self, today=datetime.date.today()):
        filename = "public_notices_%s.html" % datetime.date.today()
        path = os.path.join(idem_settings.noticedir, filename)
        return path


class Notice(tea_core.Document):

    def __init__(self,from_input=False):
        super(EnforcementDoc, self).__init__(**arguments)
        self.raw = ""
        self.url = ""
        self.body = ""
        self.title = ""
        self.newspaper = ""
        self.dates = []
        if from_input is not False:
            self.raw = from_input
            self.process_input(from_input)

    def process_input(self, content):
        domain = "http://in.mypublicnotices.com"
        link = content.split('<a href="', 1)[1].split('"')[0]
        self.url = domain + link
        body_chunk = content.split('<img')[1].split("<br>")[1].split("</td>")[0]
        if "<B>" in body_chunk:
            self.title = body_chunk.split("<B>")[1].split("</B>")[0]
            self.body = body_chunk.strip()
        if "Appeared in:" in content:
            newspaper_chunk = content.split("Appeared in:")[1].split("</td>")[0]
            self.newspaper = newspaper_chunk.split("<i>")[1].split("</i>")[0]
            formatted_dates = re.findall("\d\d/\d\d/\d\d\d\d",newspaper_chunk)
            for date_string in formatted_dates:
                new_date = datetime.datetime.strptime(date_string, "%m/%d/%Y")
                self.dates.append(new_date)
           # "Appeared in: <b><i>The Times</i></b> on 01/12/2018 and 01/19/2018<"


def get_top_page_number(page):
    def split_me(x):
        return x.split(")")[0]
    pagenumbers = [split_me(x) for x in page.split('javascript:JumpToResultsPage(')[1:]]
    pagenumbers = [int(x) for x in pagenumbers]
    top_page_number = sorted(pagenumbers)[-1]
    return top_page_number


def daily_action():
    fetcher = Fetcher()
    fetcher.fetch_all()

### clustering

# K-Means

# MOG

# Collapsed Gibbs