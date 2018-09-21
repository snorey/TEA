import unittest
import idem


class DocumentCollectionTestCase(unittest.TestCase):

    def setUp(self):
        self.document_list = [idem.Document(), idem.Document(id="100"), idem.Document(id="200"),
                              idem.Document(id="100")]
        self.collection = idem.DocumentCollection(self.document_list)
        self.doc100 = idem.Document(id="100")
        self.new_document = idem.Document(id="300", type="foo", program="bar")
        self.new_list = [idem.Document(id="400"), idem.Document(id="500"), idem.Document(id="600")]

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

    def extends_valid_list(self):
        collection = idem.DocumentCollection(self.document_list)
        length1 = len(collection)
        collection.extend(self.new_list)
        length2 = len(collection)
        self.assertEqual(length2, length1 + len(self.new_list))

    def does_not_extend_invalid_list(self):
        bad_list = [1, 2, 3]
        length1 = len(self.collection)
        self.collection.extend(bad_list)
        length2 = len(self.collection)
        self.assertEqual(length1, length2)

    def validates_items_at_init(self):
        bad_item = idem.Facility()  # class mismatch
        self.assertRaises(TypeError, idem.DocumentCollection, [bad_item])


if __name__ == '__main__':
    unittest.main(verbosity=2)
