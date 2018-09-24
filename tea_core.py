import datetime
import geojson  # pip install geojson
import idem_settings
import os
import re
import shapefile  # pip install pyshp
from shapely.geometry import mapping, Polygon, Point, MultiPoint  # pip install shapely
import time
import urllib
import urllib2
import utm  # pip install utm


DEFAULT_SHORT_WAIT = 0.3
DEFAULT_WAIT = 3
DEFAULT_WAIT_AFTER_ERROR = 30
TIMEOUT = 100
RETRY_LIMIT = 10
NUM_COORD_DIGITS = 3
DEFAULT_BUFFER = 0.015

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

    def latlongify(self):
        latlongify(self)


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
        assign_values(self, arguments, tolerant=True)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        if self.filename:
            return hash(self.filename)
        elif self.url:
            return hash(self.url)
        else:
            return hash(self.id)

    @property
    def date(self):
        if self.file_date:
            return self.file_date
        else:
            return self.crawl_date

    def retrieve_patiently(self, path="", url=None):
        if not path:
            path = self.path
        if url is None:
            url = self.url
        done = False
        inc = 0
        while not done:
            inc += 1
            if inc > RETRY_LIMIT:
                print "Aborting!"
                return False
            try:
                urllib.urlretrieve(url, path)
            except Exception, e:
                print str(e)
                time.sleep(DEFAULT_WAIT_AFTER_ERROR)
            else:
                done = True
                time.sleep(DEFAULT_WAIT)

    def to_tsv(self):
        data = [self.crawl_date.isoformat(), self.filename, self.url]
        tsv = "\t".join(data)
        return tsv

    def to_geojson(self):
        pass


def assign_values(obj, arguments, tolerant=True, cautious=True):
    for key, value in arguments.items():
        if not tolerant:  # if accept unicode and string only
            if not isinstance(value, basestring):
                print "Ignoring value %s for %s" % (str(value), str(key))
                continue
        if cautious:
            if not hasattr(obj, key):
                print "Ignoring value %s for %s, not in object properties" % (str(value), str(key))
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
        lat, lon = apply_data_to_facility(facility, result)
        return lat, lon


def apply_data_to_facility(facility, data):
    if not data:
        return
    latitude, longitude, google_address = data
    facility.full_address = google_address
    lat = float(latitude)
    lon = float(longitude)
    facility.latlong = (lat, lon)
    return lat, lon


def convert_point_to_latlong(coords):
    x, y = coords
    lat, lon = utm.to_latlon(x, y, 16, "N")
    return lat, lon


def convert_list_to_latlong(points, reverse=False):
    converted = [convert_point_to_latlong(x) for x in points]
    if reverse:
        converted = reverse_coords(converted)
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


def container_to_feature(container, props=None):
    if props is None:
        props = {"name": ""}
    points = mapping(container)["coordinates"][0]
    poly = geojson.Polygon(points)
    feature = geojson.Feature(geometry=poly, properties=props)
    return feature


def get_name_and_container(shaperecord, longlat=True):
    zip_name = shaperecord.record[0]
    utm_points = shaperecord.shape.points
    latlongs = convert_list_to_latlong(utm_points)
    if longlat:
        latlongs = [(x[1], x[0]) for x in latlongs]
    zip_container = Polygon(latlongs).buffer(0)
    return zip_name, zip_container


def get_zips(path=idem_settings.zippath, target_zips=idem_settings.lake_zips):
    shaperecords = shapefile.Reader(path).shapeRecords()
    zipdic = {}
    for s in shaperecords:
        zip_name, zip_container = get_name_and_container(s)
        if target_zips:
            if zip_name not in target_zips:
                continue
        zipdic[zip_name] = zip_container
    return zipdic


def filter_json_by_geography(json, shape):  # assumes json is FeatureCollection of Features that are points
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
        if not is_feature_in_container(feature, container):
            continue
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


def get_county_poly(countyname="Lake"):  # for http://maps.indiana.edu/download/Reference/PLSS_Counties.zip
    countypath = idem_settings.countypath
    reader = shapefile.Reader(countypath)
    county = [x for x in reader.shapeRecords() if x.record[3] == countyname][0]
    points = convert_list_to_latlong(county.shape.points)
    points = [(x[1], x[0]) for x in points]  # census uses (lon, lat)
    poly = Polygon(points)
    return poly


def find_places_in_county(countypoly=None, countyname="Lake"):
    if countypoly is None:
        countypoly = get_county_poly(countyname)
    placespath = idem_settings.placespath  # https://www2.census.gov/geo/tiger/GENZ2017/shp/cb_2017_18_place_500k.zip
    places = shapefile.Reader(placespath).shapeRecords()
    local_places = []
    for p in places:
        poly = Polygon(p.shape.points)
        placename = p.record[5]
        centroid = poly.centroid
        if countypoly.contains(centroid):
            local_places.append((placename, poly))
    return local_places


def sluggify(placename):
    slug = placename.lower()
    slug = slug.replace(" ", "-")
    slug = slug.replace(".", "")
    return slug


def generate_coord_text(coords):
    coord_text = loop_coord_list(coords)
    coord_text = "[ %s ]" % coord_text
    return coord_text


def loop_coord_list(coords, text=""):
    index = 0
    for c in coords:
        newtext = ""
        if index != 0:
            newtext = ", "
        newtext += textify_coord_pair(c)
        text += newtext
        index += 1
    return text


def textify_coord_pair(pair):
    text1 = str(pair[0])
    text2 = str(pair[1])
    text = "[%s, %s]" % (text1, text2)
    return text


def generate_centroid_text(polygon, reverse=True):
    centroid = polygon.centroid.coords[0]
    centroid = tuple([round(x, 3) for x in centroid])
    if reverse:
        centroid = (centroid[1], centroid[0])
    centroid_text = "[%s, %s]" % centroid
    return centroid_text


def reverse_coords(coord_list):
    done = [(x[1], x[0]) for x in coord_list]
    return done


def create_polygon_js(polygon, reverse=True, color="white", name=None):
    try:
        coords = polygon.boundary.coords
    except NotImplementedError:  # MultiLineString boundary
        print "Trouble!"
        coords = []
        for boundary in polygon.boundary:
            coords.extend(boundary.coords)
    if reverse:
        coords = reverse_coords(coords)
    coord_text = generate_coord_text(coords)
    centroid_text = generate_centroid_text(polygon)
    template = "var coords = %s; \n" \
               "var polygon = L.polygon(coords, {color: '%s'});\n" \
               "var centroid = %s;\n"
    js = template % (coord_text, color, centroid_text)
    if name is not None:
        js += declare_name_and_shortname(name)
    return js


def declare_name_and_shortname(name):
    nameline = 'var placename = "%s";\n' % name
    shortname = get_shortname(name)
    shortnameline = 'var shortname = "%s";\n' % shortname
    result = nameline + shortnameline
    return result


def get_shortname(placename):
    slug = sluggify(placename)
    if "-" in slug:
        pieces = slug.split("-")
        firsts = [x[0].upper() for x in pieces]
        shortname = "".join(firsts)
    else:
        shortname = placename
    return shortname


def copy_file(source, destination):
    handle = open(source)
    with open(destination, "w") as target:
        target.write(handle.read())


def setup_locality(placename, polygon, main_directory=None):
    if main_directory is None:
        main_directory = idem_settings.websitedir
    slug = sluggify(placename)
    directory = os.path.join(main_directory, slug)
    if not os.path.exists(directory):
        os.mkdir(directory)
    filepath = os.path.join(directory, "polygon.js")
    polygon_js = create_polygon_js(polygon, name=placename)
    open(filepath, "w").write(polygon_js)
    indexpath = os.path.join(main_directory, "index.html")
    target = os.path.join(directory, "index.html")
    copy_file(indexpath, target)
    for filename in ["latest_vfc.json", "latest_permits.json", "latest_enforcement.json"]:
        inpath = os.path.join(main_directory, filename)
        result = filter_json_by_polygon(inpath, polygon, directory=directory)
        print result


def get_poly_for_zip(zipcode, zippath=None, for_leaflet=True):
    if zippath is None:
        zippath = idem_settings.zippath
    r = shapefile.Reader(zippath)
    records = [x for x in r.shapeRecords() if x.record[0]==zipcode]
    print zipcode, len(records)
    if len(records) == 1:
        points = records[0].shape.points
        latlongs = convert_list_to_latlong(points, reverse=True)
        poly = Polygon(latlongs)
    else:
        points = []
        for rec in records:
            points.extend(rec.shape.points)
        if not points or len(points) < 3:
            return
        latlongs = convert_list_to_latlong(points, reverse=True)
        poly = MultiPoint(latlongs).envelope
    return poly


def recalculate_zips(zips=idem_settings.lake_zips):
    polys = [(x, get_poly_for_zip(x)) for x in zips]
    for zipcode, poly in polys:
        if poly is None:
            continue
        setup_locality(zipcode, poly)


def get_json_paths(date=None):
    import idem
    import enforcement
    import permits
    paths = []
    for module in [idem, enforcement, permits]:
        if date is None:
            path = module.latest_json_path
        else:
            path = module.get_json_filepath(date)
        paths.append(path)
    # add new JSON layers here
    return paths


def filter_json_by_polygon(jsonpath, poly, buff=DEFAULT_BUFFER, directory=None):
    """
    Filter an existing JSON file and either return result or save to corresponding filename in new directory.
    :param jsonpath: path to existing JSON file (covering a larger area such as the county)
    :param poly: polygon for smaller area
    :param buff: amount of buffering to avoid arbitrary exclusion
    :param directory: directory in which the finished file will be saved, if any
    :return: str
    """
    filtered = []
    # retrieve, clean up and parse JSON file
    jsontext = open(jsonpath).read()
    declaration, jsontext = jsontext.split(" = ", 1)
    json = geojson.loads(jsontext)
    # filter features by whether included in buffered polygon
    for feature in json.features:
        point = Point(feature.geometry.coordinates)
        if poly.buffer(buff).contains(point):
            filtered.append(feature)
    # obtain finished JSON object
    collection = geojson.FeatureCollection(filtered)
    # render back into string form
    filtered_text = geojson.dumps(collection)
    filtered_text = declaration + " = " + filtered_text
    # save to file
    if directory is None:
        return filtered_text
    else:
        filename = os.path.split(jsonpath)[-1]
        filepath = os.path.join(directory, filename)
        open(filepath, "w").write(filtered_text)
        return filepath


def filter_local_directories(root=idem_settings.websitedir):
    directories = os.listdir(root)
    directories = [os.path.join(root, x) for x in directories]
    directories = [x for x in directories if os.path.isdir(x)]
    directories = [x for x in directories if "polygon.js" in os.listdir(x)]
    return directories


def get_root_files(root=idem_settings.websitedir):
    indexpath = os.path.join(root, "index.html")
    indexfile = open(indexpath).read()
    timepath = os.path.join(root, "timestamp.js")
    if os.path.exists(timepath):
        timefile = open(timepath).read()
    else:
        timefile = None
    return indexfile, timefile


def extract_coords_from_polygon_js(filepath, reverse=True):
    lines = [x for x in open(filepath) if x.startswith("var coord")]
    if not lines:
        print "Unable to find coords!", filepath
        return
    coord_text = lines[0].split("=", 1)[1]
    finder = "\[\s*([\d\.\-]+),\s*([\d\.\-]+)\s*\]"
    coord_pieces = re.findall(finder, coord_text)
    if not coord_pieces:
        print "Arghhh!"
        return
    coords = [(float(x[0]), float(x[1])) for x in coord_pieces]
    if reverse:
        coords = reverse_coords(coords)
    return coords


def give_us_date(date):
    pattern = "%B %d, %Y"
    usdate = date.strftime(pattern)
    return usdate


def timestamp_directory(directory, date=None):
    if date is None:
        date = datetime.date.today()
    usdate = give_us_date(date)
    text = "var timestamp = '%s';" % usdate
    filepath = os.path.join(directory, "timestamp.js")
    open(filepath, "w").write(text)
    return filepath


def update_local_directory(directory, indexfile=None, timefile=None):
    # get coords from existing polygon.js
    polypath = os.path.join(directory, "polygon.js")
    coords = extract_coords_from_polygon_js(polypath)
    polygon = Polygon(coords)
    # copy index.html from root to all subs
    if indexfile is not None:
        newindexpath = os.path.join(directory, "index.html")
        open(newindexpath, "w").write(indexfile)
    # filter all json
    for path in get_json_paths():
        filter_json_by_polygon(path, polygon, directory=directory)
    # update timestamp
    if timefile is None:
        timestamp_directory(directory)
    else:
        new_timepath = os.path.join(directory, "timestamp.js")
        open(new_timepath, "w").write(timefile)


def update_all_local_directories(root=idem_settings.websitedir):
    # set this to run whenever main directory updated
    directories = filter_local_directories(root)
    indexfile, timefile = get_root_files(root)
    for directory in directories:
        print directory
        update_local_directory(directory, indexfile, timefile)


def get_daily_filepath(suffix, date=None, directory=idem_settings.maindir, doctype="permits"):
    if date is None:
        date = datetime.date.today()
    isodate = date.isoformat()
    pattern = "%s_%s.%s"
    filename = pattern % (doctype, isodate, suffix)
    filepath = os.path.join(directory, filename)
    return filepath


def do_cron():
    update_all_local_directories()

if __name__ == "__main__":
    do_cron()