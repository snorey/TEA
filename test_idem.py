import datetime
import idem
import unittest


class DocumentTestCase(unittest.TestCase):

    def setUp(self):
        self.docid = "101"
        self.facility = "argle"
        self.date1 = datetime.date(2018, 1, 1)
        self.date2 = datetime.date(2018, 10, 1)

    def tearDown(self):
        pass

    def test_same_filenames_are_equal(self):
        doc1 = idem.Document(filename="foo")
        doc2 = idem.Document(filename="bar")
        self.assertNotEqual(doc1, doc2)

    def test_empty_filenames_are_unequal(self):
        doc1 = idem.Document(filename="", id=self.docid)
        doc2 = idem.Document(filename="", id="10201")
        self.assertNotEqual(doc1, doc2)

    def test_earlier_file_date_is_less_than_later(self):
        doc1 = idem.Document(file_date=self.date1)
        doc2 = idem.Document(file_date=self.date2)
        self.assertLess(doc1, doc2)

    def test_identity_is_filename(self):
        doc = idem.Document(id=self.docid, facility=self.facility)
        self.assertEqual(doc.identity, doc.filename)

    def test_filename_escapes_slashes(self):
        doc = idem.Document(id=self.docid, program="Foo/Bar", type="Meh")
        self.assertFalse("/" in doc.filename)

    def test_latest_date_returns_filedate_if_crawldate_is_null(self):
        doc = idem.Document(file_date=self.date1)
        self.assertEqual(doc.latest_date, self.date1)


class DocumentCollectionTestCase(unittest.TestCase):

    def setUp(self):
        self.document_list = [idem.Document(), idem.Document(id="100"), idem.Document(id="200"),
                              idem.Document(id="100")]
        self.collection = idem.DocumentCollection(self.document_list)
        self.doc100 = idem.Document(id="100")
        self.new_document = idem.Document(id="300", type="foo", program="bar")
        self.new_list = [idem.Document(id="400"), idem.Document(id="500"), idem.Document(id="600")]
        self.bad_item = idem.Facility()  # class mismatch

    def tearDown(self):
        self.collection = None
        self.document_list = None

    def test_duplicates_deleted_on_creation(self):
        length = len(self.document_list)
        collection = idem.DocumentCollection(self.document_list)
        self.assertEqual(len(collection), length-1)

    def test_removes_deleted_item(self):
        length = len(self.collection)
        self.collection.remove(idem.Document())
        self.assertEqual(len(self.collection), length-1)

    def test_non_document_raises_TypeError(self):
        self.assertRaises(TypeError, self.collection.append, "document")

    def test_updates_types(self):
        self.collection.append(self.new_document)
        self.assertTrue(self.new_document.type in self.collection.types)
        self.assertTrue(self.new_document.program in self.collection.programs)

    def test_does_not_validate_duplicate(self):
        self.assertFalse(self.collection.validate_item(self.doc100))

    def test_extends_valid_list(self):
        collection = idem.DocumentCollection(self.document_list)
        length1 = len(collection)
        collection.extend(self.new_list)
        length2 = len(collection)
        self.assertEqual(length2, length1 + len(self.new_list))

    def test_raises_TypeError_for_invalid_list(self):
        bad_list = [1, 2, 3]
        self.assertRaises(TypeError, self.collection.extend, bad_list)

    def test_validates_items_at_init(self):
        self.assertRaises(TypeError, idem.DocumentCollection, [self.bad_item])


class FacilityCollectionTestCase(unittest.TestCase):

    def setUp(self):
        self.facility_list = [idem.Facility(), idem.Facility(vfc_id="100"), idem.Facility(vfc_id="200"),
                              idem.Facility(vfc_id="100")]
        self.collection = idem.FacilityCollection(self.facility_list)
        self.fac100 = idem.Facility(vfc_id="100")
        self.new_fac = idem.Facility(id="300", vfc_name="Argle Bargle")
        self.new_list = [idem.Facility(vfc_id="400"), idem.Facility(vfc_id="500"), idem.Facility(vfc_id="600")]
        self.bad_item = idem.Document()  # class mismatch
        self.bad_list = [1, 2, 3]

    def tearDown(self):
        self.collection = None
        self.facility_list = None

    def test_fc_duplicates_deleted_on_creation(self):
        length = len(self.facility_list)
        collection = idem.FacilityCollection(self.facility_list)
        self.assertEqual(len(collection), length-1)

    def test_fc_removes_deleted_item(self):
        length = len(self.collection)
        self.collection.remove(idem.Facility())
        self.assertEqual(len(self.collection), length-1)

    def test_fc_non_document_raises_TypeError(self):
        self.assertRaises(TypeError, self.collection.append, "document")

    def test_fc_updates_names_and_ids(self):
        new_fac = idem.Facility(id="300", vfc_name="Argle Bargle")
        self.collection.append(new_fac)
        self.assertTrue(new_fac.vfc_id in self.collection.iddic.keys())
        self.assertTrue(new_fac.vfc_name in self.collection.namedic.keys())

    def test_fc_does_not_validate_duplicate(self):
        self.assertFalse(self.collection.validate_item(self.fac100))

    def test_fc_extends_valid_list(self):
        collection = idem.FacilityCollection(self.facility_list)
        length1 = len(collection)
        collection.extend(self.new_list)
        length2 = len(collection)
        self.assertEqual(length2, length1 + len(self.new_list))

    def test_fc_raises_TypeError_for_invalid_list(self):
        self.assertRaises(TypeError, self.collection.extend, self.bad_list)

    def test_fc_validates_items_at_init(self):
        self.assertRaises(TypeError, idem.FacilityCollection, [self.bad_item])


if __name__ == '__main__':
    unittest.main(verbosity=2)
