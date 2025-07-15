#!/usr/bin/env python3
"""
Klaviyo Review Event Extractor

This script fetches "Submitted Review" events from the Klaviyo API and extracts
CQ fields, review fields, and product details to a CSV file.
"""

import requests
import pandas as pd
import time
from typing import Dict, List, Any, Optional
import argparse
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class KlaviyoReviewExtractor:
    def __init__(self, api_key: str):
        """Initialize the Klaviyo API client."""
        self.api_key = api_key
        self.base_url = "https://a.klaviyo.com/api"
        self.headers = {
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "revision": "2024-10-15"
        }
        
    def get_event_by_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific event by its ID to access full event properties."""
        try:
            url = f"{self.base_url}/events/{event_id}"
            params = {"include": "metric"}
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Event not found or error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error fetching event by ID: {e}")
            return None

    def get_review_metric_id(self) -> Optional[str]:
        """Get the metric ID for Submitted review events."""
        try:
            url = f"{self.base_url}/metrics"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            metrics = data.get("data", [])
            
            for metric in metrics:
                metric_name = metric.get("attributes", {}).get("name")
                if metric_name == "Submitted review":
                    return metric.get("id")
            
            print("Submitted review metric not found. Please check if Klaviyo Reviews is enabled.")
            return None
            
        except Exception as e:
            print(f"Error fetching metrics: {e}")
            return None

    def fetch_review_events(self, start_date: str, end_date: str, metric_id: str) -> List[Dict[str, Any]]:
        """Fetch review events from Klaviyo API with pagination support."""
        all_events = []
        url = f"{self.base_url}/events"
        page_count = 0
        
        # Convert dates to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"
        
        # Build filter with metric ID and date range
        filters = [
            f"equals(metric_id,'{metric_id}')",
            f"greater-or-equal(datetime,{start_datetime})",
            f"less-or-equal(datetime,{end_datetime})"
        ]
        
        params = {
            "filter": ",".join(filters),
            "page[size]": 200,
            "sort": "datetime",
            "include": "metric"
        }
        
        print(f"Fetching events from {start_date} to {end_date}...")
        
        while url:
            try:
                response = requests.get(url, headers=self.headers, params=params if url == f"{self.base_url}/events" else None)
                response.raise_for_status()
                
                data = response.json()
                events = data.get("data", [])
                all_events.extend(events)
                page_count += 1
                
                print(f"Page {page_count}: Found {len(events)} events (total: {len(all_events)})")
                
                # Get next page URL from links
                links = data.get("links", {})
                url = links.get("next")
                params = None  # Clear params for subsequent requests
                
                # Rate limiting - be respectful to the API
                time.sleep(0.01)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching events: {e}")
                break
        
        print(f"✓ Completed fetching {len(all_events)} events from {page_count} pages")
        return all_events

    def get_detailed_event_data(self, event_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch detailed event data for a list of event IDs."""
        detailed_events = []
        total_events = len(event_ids)
        
        print(f"Fetching detailed data for {total_events} events...")
        
        for i, event_id in enumerate(event_ids, 1):
            event_data = self.get_event_by_id(event_id)
            if event_data:
                detailed_events.append(event_data.get("data"))
            
            # Show progress every 10 events or at the end
            if i % 10 == 0 or i == total_events:
                print(f"Progress: {i}/{total_events} events processed ({i/total_events*100:.1f}%)")
            
            # Rate limiting between individual requests
            time.sleep(0.1)
        
        print(f"✓ Completed fetching detailed data for {len(detailed_events)} events")
        return detailed_events
    
    def generate_date_chunks(self, start_date: str, end_date: str, chunk_months: int = 1) -> List[tuple]:
        """Generate date chunks for processing large date ranges."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        chunks = []
        current_start = start_dt
        
        while current_start <= end_dt:
            current_end = min(current_start + relativedelta(months=chunk_months) - timedelta(days=1), end_dt)
            chunks.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))
            current_start = current_end + timedelta(days=1)
        
        return chunks
    
    def process_date_range_in_chunks(self, start_date: str, end_date: str, metric_id: str, detailed: bool = False) -> List[Dict[str, Any]]:
        """Process a large date range by splitting into monthly chunks."""
        chunks = self.generate_date_chunks(start_date, end_date)
        all_extracted_data = []
        
        print(f"Processing date range {start_date} to {end_date} in {len(chunks)} monthly chunks...")
        
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            print(f"\n--- Chunk {i}/{len(chunks)}: {chunk_start} to {chunk_end} ---")
            
            # Fetch events for this chunk
            chunk_events = self.fetch_review_events(chunk_start, chunk_end, metric_id)
            
            if not chunk_events:
                print(f"No events found in chunk {i}")
                continue
            
            # Get detailed data if requested
            if detailed:
                event_ids = [event.get("id") for event in chunk_events]
                detailed_events = self.get_detailed_event_data(event_ids)
                chunk_events = detailed_events
            
            # Extract data from this chunk
            chunk_data = self.extract_review_data(chunk_events)
            all_extracted_data.extend(chunk_data)
            
            print(f"Chunk {i} completed: {len(chunk_data)} reviews extracted")
        
        print(f"\n✓ All chunks completed! Total reviews: {len(all_extracted_data)}")
        return all_extracted_data

    def extract_review_data(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract relevant data from review events."""
        extracted_data = []
        
        for event in events:
            attributes = event.get("attributes", {})
            # Use event_properties for individual event calls, properties for bulk calls
            properties = attributes.get("event_properties", {}) or attributes.get("properties", {})
            
            # Initialize row data
            row_data = {
                "event_id": event.get("id"),
                "event_datetime": attributes.get("datetime"),
                "profile_email": attributes.get("profile", {}).get("data", {}).get("attributes", {}).get("email")
            }
            
            # Extract CQ fields (fields starting with "CQ:")
            for key, value in properties.items():
                if key.startswith("CQ:"):
                    # Handle list values by joining them
                    if isinstance(value, list):
                        row_data[key] = ", ".join(map(str, value))
                    else:
                        row_data[key] = value
            
            # Extract review fields
            review_fields = [
                "review_verified", "review_email", "review_id", "review_rating",
                "review_author", "review_status", "review_has_media", 
                "review_content", "review_title", "review_link", "is_store_review"
            ]
            
            for field in review_fields:
                row_data[field] = properties.get(field)
            
            # Extract product details
            product_data = properties.get("product", {})
            if product_data:
                variant_data = product_data.get("variant") or {}
                row_data.update({
                    "product_id": product_data.get("id"),
                    "product_title": product_data.get("title"),
                    "product_handle": product_data.get("handle"),
                    "product_type": product_data.get("product_type"),
                    "product_vendor": product_data.get("vendor"),
                    "product_tags": product_data.get("tags"),
                    "variant_id": variant_data.get("id"),
                    "variant_title": variant_data.get("title"),
                    "variant_sku": variant_data.get("sku")
                })
            
            # Extract structured product details if available
            structured_product = properties.get("structured_product", {})
            if structured_product:
                row_data.update({
                    "structured_product_name": structured_product.get("product_name"),
                    "structured_product_url": structured_product.get("url"),
                    "structured_product_image_url": structured_product.get("image_url")
                })
            
            extracted_data.append(row_data)
            
        return extracted_data
    
    def save_to_csv(self, data: List[Dict[str, Any]], filename: str) -> None:
        """Save extracted data to CSV file."""
        if not data:
            print("No data to save.")
            return
            
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        print(f"Total records: {len(data)}")


def main():
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Extract Klaviyo Submitted Review events")
    parser.add_argument("--api-key", help="Klaviyo API key (or set KLAVIYO_API_KEY in .env file)")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="klaviyo_reviews.csv", help="Output CSV filename")
    parser.add_argument("--detailed", action="store_true", help="Fetch detailed event data (slower but more complete)")
    
    args = parser.parse_args()
    
    # Get API key from args or environment variable
    api_key = args.api_key or os.getenv("KLAVIYO_API_KEY")
    if not api_key:
        print("Error: API key required. Use --api-key argument or set KLAVIYO_API_KEY in .env file.")
        return
    
    # Initialize extractor
    extractor = KlaviyoReviewExtractor(api_key)
    
    # Get the metric ID for "Submitted review"
    print("Looking up Submitted review metric...")
    metric_id = extractor.get_review_metric_id()
    if not metric_id:
        return
    
    print(f"Found metric ID: {metric_id}")
    
    # Process date range in chunks
    extracted_data = extractor.process_date_range_in_chunks(
        args.start_date, 
        args.end_date, 
        metric_id, 
        detailed=args.detailed
    )
    
    if not extracted_data:
        print("No review events found in the specified date range.")
        return
    
    # Save to CSV
    extractor.save_to_csv(extracted_data, args.output)


if __name__ == "__main__":
    main()