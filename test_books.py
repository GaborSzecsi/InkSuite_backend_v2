import json
import os

# Test loading the books file directly
books_file = "marble_app/book_data/books.json"

print(f"Books file exists: {os.path.exists(books_file)}")
print(f"Books file path: {os.path.abspath(books_file)}")

if os.path.exists(books_file):
    with open(books_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        print(f"Number of books loaded: {len(data)}")
        if data:
            print(f"First book title: {data[0].get('title', 'No title')}")
            print(f"First book author: {data[0].get('author', 'No author')}")
else:
    print("Books file not found!") 
