import datetime
import geojson  # pip install geojson
import idem_settings
import os
import re
import shapefile  # pip install pyshp
from shapely.geometry import mapping, Polygon, Point  # pip install shapely
import time
import urllib
import urllib2
import utm  # pip install utm


DEFAULT_WAIT = 3
DEFAULT_WAIT_AFTER_ERROR = 30
TIMEOUT = 100
RETRY_LIMIT = 10
NUM_COORD_DIGITS = 3

# core functionality for TEA project
# certain concepts are consistent across applications:


class Facility(object):
    name = ""
    city = ""
    street_address = ""
    county = ""
    state = ""
    zip = ""
    latlong = ()
    full_address = ""
    vfc_id = ""
    vfc_url = ""
    vfc_name = ""
    vfc_address = ""
    docs = []
    updated_docs = set()
    downloaded_docs = set()
    directory = ""
    row = ""
    page = ""
    date = datetime.date.today()

    def __init__(self, **arguments):
        assign_values(self, arguments, tolerant=True)
        if "directory" in arguments.keys():
            self.set_directory(directory=arguments["directory"])
        else:
            self.set_directory()
        if "parent" in arguments.keys():
            parent = arguments["parent"]
            self.zip = parent.zip
            self.parent = parent
        self.docs = self.get_docs()
        self.get_latest_page()
        if not self.downloaded_docs:
            self.downloaded_docs = self.get_downloaded_docs()
        self.docs = self.docs_from_page(self.page)

    def __eq__(self, other):
        if hasattr(other, "identity"):
            return self.identity == other.identity
        else:
            return False

    def __hash__(self):
        return hash(self.identity)

    @property
    def identity(self):
        return self.name, self.city, self.latlong

    @property
    def latitude(self):
        return float(self.latlong[0])

    @property
    def longitude(self):
        return float(self.latlong[1])

    def set_directory(self, directory=False):
        if directory:
            self.directory = directory
        elif hasattr(self, "parent"):
            self.directory = os.path.join(self.parent.directory, self.vfc_id)
        else:
            self.directory = self.vfc_id
        if self.directory:
            if not os.path.isdir(self.directory):
                os.mkdir(self.directory)
        return self.directory

    def get_docs(self):
        if self.directory:
            return set(os.listdir(self.directory))
        else:
            return set()

    def get_downloaded_docs(self):
        if self.directory:
            return set(os.listdir(self.directory))
        else:
            return set()

    @staticmethod
    def get_latest_page():
        pass

    @staticmethod
    def retrieve_page():
        return ""

    def docs_from_page(self, page=""):
        if not hasattr(self, page) or not page:
            self.page = page
        return set()


class Document(object):
    url = ""
    crawl_date = datetime.date.today()
    file_date = False
    id = ""
    doc_type = ""
    program = ""
    filename = ""
    facility = ""
    path = ""
    content = ""

    def __init__(self, **arguments):
        assign_values(self, arguments)

    def __eq__(self, other):
        if type(other) == str:
            return self.filename == other
        else:
            return self.filename == other.filename

    def __hash__(self):
        return hash(self.filename)

    @property
    def date(self):
        if self.file_date:
            return self.file_date
        else:
            return self.crawl_date

    def retrieve_patiently(self, path=""):
        if not path:
            path = self.path
        done = False
        inc = 0
        while not done:
            inc += 1
            if inc > RETRY_LIMIT:
                print "Aborting!"
                return False
            try:
                urllib.urlretrieve(self.url, path)
            except Exception, e:
                print str(e)
                time.sleep(DEFAULT_WAIT_AFTER_ERROR)
            else:
                done = True
                time.sleep(DEFAULT_WAIT)

    def to_tsv(self):
        data = [self.crawl_date, self.filename, self.url]
        tsv = "\t".join(data)
        return tsv

    def to_geojson(self):
        pass


def assign_values(obj, arguments, tolerant=False):
    for key, value in arguments.items():
        if not tolerant:  # if accept unicode and string only
            if not isinstance(value, basestring):
                print "Ignoring value %s for %s" % (str(value), str(key))
                continue
        setattr(obj, key, value)


def get_previous_file_in_directory(directory,
                                   pattern=".*(\d{4}-\d{2}-\d{2})",
                                   reference_date=datetime.date.today().isoformat()):
    # note that this function requires that the directory have only one file matching the pattern per date
    def is_dated_file(file_in_directory):
        return re.match(pattern, file_in_directory)
    if not isinstance(reference_date, basestring):
        reference_date = reference_date
    files = os.listdir(directory)
    files = [x for x in files if is_dated_file(x)]
    dated_files = []
    for filename in files:
        date = re.search(pattern, filename).group(1)
        dated_files.append((date, filename))
    dated_files.sort()
    dated_files.reverse()
    for date, filename in dated_files:  # starting from most recent
        if date < reference_date:
            filepath = os.path.join(directory, filename)
            return filepath


def do_patiently(action, *args, **kwargs):
    done = False
    inc = 0
    result = None
    while not done:
        inc += 1
        if inc > RETRY_LIMIT:
            print "Aborting!"
            return False
        try:
            result = action(*args, **kwargs)
        except Exception, e:
            print str(e)
            time.sleep(DEFAULT_WAIT_AFTER_ERROR)
        else:
            done = True
            time.sleep(DEFAULT_WAIT)
    return result


def retrieve_patiently(url, path):
    do_patiently(urllib.urlretrieve, url, path)
    return path


# wordpress

def wp_login():
    from wordpress_xmlrpc import Client
    username = idem_settings.wp_user
    password = idem_settings.wp_password
    client = Client(idem_settings.wp_url, username, password)
    return client


def upload_post_text(text, publish=False, posttitle="", tags=list(), slug=""):
    from wordpress_xmlrpc import WordPressPost
    from wordpress_xmlrpc.methods.posts import NewPost, EditPost
    post = WordPressPost()
    client = wp_login()
    print "Logged in..."
    if not posttitle:
        post.title = "Notifications for %s" % datetime.date.today().strftime("%B %d, %Y")
    else:
        post.title = posttitle
    post.content = text
    if tags:
        tags = [x.lower().replace(" ", "-") for x in tags]
        tags = [re.sub("[^\w\-]", "", x) for x in tags]
        print str(tags)
        post.terms_names['post_tag'] = tags
    if not slug:
        slug = posttitle.replace(",", "").replace(" ", "-").lower()
    post.slug = slug
    done = False
    while not done:
        try:
            post.id = client.call(NewPost(post))
        except Exception, e:  # debug tag issue
            print str(e)
            time.sleep(5)
        else:
            done = True
    if publish:
        post.post_status = 'publish'
        client.call(EditPost(post.id, post))
    return post.id


def coord_from_address(address):
    apikey = idem_settings.google_maps_key
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s"
    url = url % (urllib.quote(address), apikey)
    print url
    try:
        apipage = urllib2.urlopen(url).read()
    except urllib2.HTTPError, e:  # bad request
        print str(e)
        return False
    try:
        geometry = apipage.split('"geometry"')[1].split('"location"')[1]
    except IndexError:
        return False
    latitude = geometry.split('"lat"')[1].split(':')[1].split(",")[0].split("\n")[0].strip()
    latitude = str(round(float(latitude), NUM_COORD_DIGITS))
    longitude = geometry.split('"lng"')[1].split(':')[1].split(",")[0].split("\n")[0].strip()
    longitude = str(round(float(longitude), NUM_COORD_DIGITS))
    googleadd = apipage.split('"formatted_address"')[1].split('"')[1].split('"')[0].strip()
    return latitude, longitude, googleadd


def latlongify(facility):
    address = facility.full_address
    result = coord_from_address(address)
    if result is not False:
        latitude, longitude, google_address = result
        facility.full_address = google_address
        lat = float(latitude)
        lon = float(longitude)
        facility.latlong = (lat, lon)
        return lat, lon


def convert_point_to_latlong(coords):
    x, y = coords
    lat, lon = utm.to_latlon(x, y, 16, "N")
    return lat, lon


def convert_list_to_latlong(points):
    converted = [convert_point_to_latlong(x) for x in points]
    return converted


def get_point_from_feature(feature):
    coords, = [x for x in geojson.coords(feature)]
    lon, lat = coords  # geoJSON uses reverse order
    point = Point(lat, lon)
    return point


def is_feature_in_container(feature, container):
    point = get_point_from_feature(feature)
    if container.contains(point):
        return True
    else:
        return False


def container_to_feature(container, props={"name":""}):
    points = mapping(container)["coordinates"][0]
    poly = geojson.Polygon(points)
    feature = geojson.Feature(geometry=poly, properties=props)
    return feature


def get_name_and_container(shaperecord, longlat=True):
    zip_name = shaperecord.record[0]
    utm_points = shaperecord.shape.points
    latlongs = convert_list_to_latlong(utm_points)
    if longlat:
        latlongs = [(x[1],x[0]) for x in latlongs]
    zip_container = Polygon(latlongs).buffer(0)
    return zip_name, zip_container


def get_zips(path='/home/sam/TEA/ZIP/ZCTA_TIGER05_IN.shp', target_zips=idem_settings.lake_zips):
    shaperecords = shapefile.Reader(path).shapeRecords()
    zips = {}
    for s in shaperecords:
        zip_name, zip_container = get_name_and_container(s)
        if target_zips:
            if zip_name not in target_zips:
                continue
        zips[zip_name] = zip_container
    return zips


def filter_json_by_geography(json, shape): # assumes json is FeatureCollection of Features that are points
    if not isinstance(shape, Polygon):
        container = Polygon(shape.points)
    else:
        container = shape
    if isinstance(json, basestring):
        collection = geojson.loads(json)
    else:
        collection = json
    filtered_features = []
    for feature in collection.features:
        if is_feature_in_container(feature, container):
            filtered_features.append(feature)
    container_feature = container_to_feature(container)
    filtered_features.append(container_feature)
    new_collection = geojson.FeatureCollection(filtered_features)
    return new_collection


def geojson_to_js(json, var_name="geojson"):
    text = "var %s = " % var_name
    text += json
    text += ";"
    return text


def js_to_geojson(js):
    offset = js.find("{")
    json = js[offset:].strip()
    if json.endswith(";"):
        json = json[:-1]
    return json


def get_county_poly(path="/home/sam/Downloads/Counties/tl_2013_18_cousub.shp",countycode=45):
    counties = shapefile.Reader(path).shapeRecords()
    county = [x for x in counties if x.record[-1] == int(countycode)][0]
    points = county.shape.points
    poly = Polygon(points)
    return poly