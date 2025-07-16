# Klaviyo Review Event Extractor

This script extracts "Submitted Review" events from the Klaviyo API and exports the data to a CSV file.
Built with Claude Code. 

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your API key:
```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your actual API key
# KLAVIYO_API_KEY=pk_your_actual_api_key_here
```

## Usage

```bash
python klaviyo_review_extractor.py --start-date 2024-01-01 --end-date 2024-12-31 --output reviews.csv
```

Alternatively, you can still provide the API key directly:
```bash
python klaviyo_review_extractor.py --api-key YOUR_API_KEY --start-date 2024-01-01 --end-date 2024-12-31
```

### Parameters

- `--api-key`: Your Klaviyo private API key (starts with `pk_`)
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format  
- `--output`: Output CSV filename (optional, defaults to `klaviyo_reviews.csv`)

## Extracted Data

The script extracts the following data from "Submitted Review" events:

### Event Information
- `event_id`: Unique event identifier
- `event_datetime`: When the event occurred
- `profile_email`: Email of the profile associated with the event

### CQ Fields
- All fields starting with "CQ:" (custom questions)

### Review Fields
- `review_verified`, `review_email`, `review_id`, `review_rating`
- `review_author`, `review_status`, `review_has_media`
- `review_content`, `review_title`, `review_link`, `is_store_review`

### Product Details
- `product_id`, `product_title`, `product_handle`, `product_type`
- `product_vendor`, `product_tags`
- `variant_id`, `variant_title`, `variant_sku`
- `structured_product_name`, `structured_product_url`, `structured_product_image_url`

## API Requirements

- Requires `events:read` scope on your Klaviyo API key
- Rate limited to 350 requests/second (burst) and 3,500 requests/minute (steady)
- Returns maximum 200 events per page (script handles pagination automatically)