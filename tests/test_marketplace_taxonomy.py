import unittest
from app.marketplace.catalog_taxonomy import CATALOG, matching_isbns, verified_filter

class TaxonomyTests(unittest.TestCase):
    def test_all_verified_books_are_classified_once(self):
        self.assertEqual(len(CATALOG),174)
        self.assertEqual({a:sum(b['audience']==a for b in CATALOG) for a in ['children','young-adult','adult']},
                         {'children':30,'young-adult':45,'adult':99})
        children=set(matching_isbns('children'));adult=set(matching_isbns('adult'))
        self.assertFalse(children & adult)
    def test_known_titles_do_not_leak_across_age_categories(self):
        self.assertIn('9781958325391',matching_isbns('children','Body Safety'))
        self.assertNotIn('9781958325391',matching_isbns('adult','Body Safety'))
        self.assertIn('9780744311808',matching_isbns('adult','Thrillers'))
        self.assertNotIn('9780744311808',matching_isbns('children'))
    def test_distinct_science_fiction_and_fantasy(self):
        self.assertIn('9780744305159',matching_isbns('young-adult','Fantasy'))
        self.assertNotIn('9780744305159',matching_isbns('young-adult','Science Fiction'))
    def test_unknown_selection_is_empty_and_other_publishers_keep_metadata(self):
        self.assertEqual(matching_isbns('children','Wrong category'),[])
        self.assertIsNone(verified_filter('another-publisher','children','','JOIN editions e'))
        clause,params=verified_filter('marble-press','children','Picture Books','JOIN editions e')
        self.assertIn('be.marketplace_book_id=b.id',clause)
        self.assertIn('e.isbn13=ANY(%s)',clause)
        self.assertEqual(len(params),1)

if __name__=='__main__': unittest.main()

