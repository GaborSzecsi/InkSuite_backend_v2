"""Merchant-reviewed Shopify categories, matched by ISBN rather than title guesses."""
import json
from pathlib import Path

CATALOG = json.loads(Path(__file__).with_name('marble_press_catalog.json').read_text(encoding='utf-8'))
GENRES = {'Adventure':'adventure','Fantasy':'fantasy','Graphic Novels':'graphic-novels',
          'Historical':'historical','Horror':'horror','Literary':'literary','Mystery':'mystery',
          'Nonfiction':'nonfiction','Paranormal':'paranormal','Romance':'romance',
          'Science Fiction':'science-fiction','Thrillers':'thrillers','Body Safety':'body-safety',
          'Emotions':'emotions','Middle Grade':'middle-grade','Picture Books':'picture-books'}

def matching_isbns(audience='', category=''):
    audience = 'children' if audience == 'kids' else audience
    genres = {'fantasy','science-fiction'} if category == 'Sci-Fi & Fantasy' else {'mystery','thrillers'} if category == 'Mystery & Thrillers' else {GENRES.get(category)} if category else set()
    return [isbn for book in CATALOG if (not audience or book['audience']==audience)
            and (not genres or genres.intersection(book['genres'])) for isbn in book['isbns']]

def verified_filter(publisher, audience, category, edition_join):
    if publisher != 'marble-press' or not (audience or category):
        return None
    # Only selected, publicly listed editions participate; tenant/work joins are supplied
    # by the existing catalog query. No unpublished edition is exposed by this filter.
    return (' AND EXISTS(SELECT 1 '+edition_join+
            ' WHERE be.marketplace_book_id=b.id AND e.isbn13=ANY(%s))',
            [matching_isbns(audience, category)])
