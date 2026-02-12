# Marble App - Book Production Manager

A comprehensive book production management system with royalty calculation capabilities, built with FastAPI backend and Next.js frontend.

## Features

### Backend (FastAPI)
- **Book Management**: Add, edit, and delete books with detailed metadata
- **Royalty Calculator**: Calculate royalties for authors and illustrators
- **Statement Management**: Save and retrieve royalty statements
- **RESTful API**: Clean, documented API endpoints

### Frontend (Next.js)
- **Book Management Page**: Full CRUD operations for books
- **Royalty Calculator**: Interactive royalty calculation interface
- **Responsive Design**: Modern, mobile-friendly UI

## Project Structure

```
marble_app/
├── models/
│   ├── book.py          # Book data models
│   └── royalty.py       # Royalty calculation models
├── services/
│   ├── file_ops.py      # File operations (JSON)
│   └── royalty_calculator.py  # Royalty calculation logic
├── routers/
│   ├── books.py         # Book management endpoints
│   └── royalty.py       # Royalty calculator endpoints
├── book_data/           # JSON data storage
├── main.py              # FastAPI application entry point
└── README.md           # This file
```

## Setup Instructions

### Backend Setup

1. **Install Dependencies**
   ```bash
   cd "C:\Users\szecs\documents\marble_app"
   pip install fastapi uvicorn pydantic
   ```

2. **Start the Server**
   ```bash
   python -m uvicorn marble_app.main:app --reload
   ```

3. **Access the API**
   - API Documentation: http://localhost:8000/docs
   - Alternative Docs: http://localhost:8000/redoc
   - API Base URL: http://localhost:8000/api

### Frontend Setup

1. **Install Dependencies**
   ```bash
   cd "C:\Users\szecs\marble-frontend"
   npm install
   ```

2. **Start the Development Server**
   ```bash
   npm run dev
   ```

3. **Access the Frontend**
   - Main App: http://localhost:3000
   - Book Management: http://localhost:3000/books
   - Royalty Calculator: http://localhost:3000/royalty

## API Endpoints

### Books
- `GET /api/books` - Get all books
- `POST /api/books` - Create/update a book
- `DELETE /api/books` - Delete a book

### Royalty Calculator
- `GET /api/royalty/books` - Get all books (for royalty calculations)
- `POST /api/royalty/books` - Save book with royalty settings
- `DELETE /api/royalty/books` - Delete a book
- `POST /api/royalty/calculate` - Calculate royalties for a period
- `POST /api/royalty/statements` - Save a royalty statement
- `GET /api/royalty/statements/{person_type}/{person_name}` - Get person's statements
- `DELETE /api/royalty/statements/{person_type}/{person_name}` - Delete a statement
- `GET /api/royalty/categories` - Get available royalty categories
- `GET /api/royalty/format-types` - Get available book format types

## Data Models

### Book
```python
{
  "title": "string",
  "author": "string",
  "author_email": "string (optional)",
  "author_address": "string (optional)",
  "publishing_year": "integer",
  "formats": [
    {
      "format": "string",
      "isbn": "string",
      "price": "float"
    }
  ],
  "author_royalty": [
    {
      "category": "string",
      "royalty_percent": "float",
      "net_revenue_based": "boolean"
    }
  ],
  "author_advance": "float",
  "illustrator": {
    "name": "string",
    "email": "string",
    "address": "string"
  },
  "illustrator_royalty": [...],
  "illustrator_advance": "float"
}
```

### Royalty Calculation Request
```python
{
  "book_id": "string (title|author)",
  "period_start": "string (MM-DD-YYYY)",
  "period_end": "string (MM-DD-YYYY)",
  "sales_data": [
    {
      "category": "string",
      "units": "integer",
      "returns": "integer",
      "unit_price_or_net_revenue": "float",
      "discount": "float",
      "net_revenue": "boolean"
    }
  ],
  "author_rates": {"category": "rate_percentage"},
  "illustrator_rates": {"category": "rate_percentage"},
  "author_advance": "float",
  "illustrator_advance": "float"
}
```

## Usage Examples

### Adding a Book
```bash
curl -X POST "http://localhost:8000/api/royalty/books" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Example Book",
    "author": "Jane Doe",
    "publishing_year": 2024,
    "author_advance": 5000.0,
    "formats": [
      {"format": "Hardcover", "isbn": "1234567890", "price": 24.99}
    ]
  }'
```

### Calculating Royalties
```bash
curl -X POST "http://localhost:8000/api/royalty/calculate" \
  -H "Content-Type: application/json" \
  -d '{
    "book_id": "Example Book|Jane Doe",
    "period_start": "01-01-2024",
    "period_end": "03-31-2024",
    "sales_data": [
      {
        "category": "Hardcover",
        "units": 1000,
        "returns": 50,
        "unit_price_or_net_revenue": 24.99,
        "discount": 0.0,
        "net_revenue": false
      }
    ],
    "author_rates": {"Hardcover": 10.0},
    "illustrator_rates": {"Hardcover": 5.0},
    "author_advance": 5000.0,
    "illustrator_advance": 2000.0
  }'
```

## Data Storage

All data is stored in JSON files in the `book_data/` directory:
- `books.json` - Book information
- `royalty_statements.json` - All royalty statements
- `author_royalties.json` - Author-specific statements
- `illustrator_royalties.json` - Illustrator-specific statements

## Development

### Adding New Features
1. Create models in `models/` directory
2. Add business logic in `services/` directory
3. Create API endpoints in `routers/` directory
4. Update `main.py` to include new routers
5. Test with the interactive API documentation

### Testing
- Use the FastAPI automatic documentation at http://localhost:8000/docs
- Test API endpoints directly in the browser
- Use tools like Postman or curl for more complex testing

## Migration from Streamlit

This FastAPI version replaces the original Streamlit app with:
- **Better Performance**: FastAPI is significantly faster than Streamlit
- **API-First Design**: Can be used by multiple frontends
- **Better Scalability**: Can handle multiple concurrent users
- **Modern Frontend**: Next.js provides a better user experience
- **Separation of Concerns**: Backend and frontend are properly separated

## Support

For issues or questions:
1. Check the API documentation at http://localhost:8000/docs
2. Review the data models and examples above
3. Test individual endpoints to isolate issues 