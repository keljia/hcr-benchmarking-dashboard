#!/usr/bin/env python3
"""Geocode HCR addresses using the US Census Geocoder batch API."""

import csv
import requests
import json
import io
import time
import sys

def geocode_batch(addresses, start_index):
    """Geocode a batch of addresses using the US Census Geocoder."""
    batch_lines = []
    for i, addr in enumerate(addresses):
        global_idx = start_index + i
        street = addr['address'].replace(',', ' ')
        city = addr['city'].replace(',', ' ')
        state = addr['state'].replace(',', ' ')
        zipcode = addr['zip'].replace(',', ' ')
        batch_lines.append(f'{global_idx},"{street}","{city}","{state}","{zipcode}"')

    batch_text = "\n".join(batch_lines)

    url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
    response = requests.post(
        url,
        files={
            'addressFile': ('addresses.csv', batch_text.encode('utf-8'), 'text/csv'),
        },
        data={
            'benchmark': 'Public_AR_Current',
            'returntype': 'locations',
        },
        timeout=180,
    )
    response.raise_for_status()

    results = []
    reader = csv.reader(io.StringIO(response.text))
    for row in reader:
        if len(row) < 4:
            continue
        match_status = row[2].strip() if len(row) > 2 else ''
        if match_status == 'Match':
            try:
                idx = int(row[0].strip())
                coords_str = row[5].strip() if len(row) > 5 else ''
                if not coords_str:
                    continue
                lon_str, lat_str = coords_str.split(',')
                results.append({
                    'index': idx,
                    'address': addresses[idx - start_index]['address'],
                    'city': addresses[idx - start_index]['city'],
                    'state': addresses[idx - start_index]['state'],
                    'zip': addresses[idx - start_index]['zip'],
                    'lat': float(lat_str.strip()),
                    'lon': float(lon_str.strip()),
                })
            except (ValueError, IndexError) as e:
                print(f"  Warning: could not parse row {row}: {e}", file=sys.stderr)

    return results


def main():
    # Load addresses
    addresses = []
    with open('HCR addresses.csv', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            addresses.append({
                'address': row['Address 1'].strip(),
                'city': row['City'].strip(),
                'state': row['State/Province'].strip(),
                'zip': row['Postal Code'].strip(),
            })

    print(f"Loaded {len(addresses)} addresses")

    all_results = []
    batch_size = 500

    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(addresses) + batch_size - 1) // batch_size
        print(f"Geocoding batch {batch_num}/{total_batches} (rows {i}–{i + len(batch) - 1})...")
        try:
            results = geocode_batch(batch, i)
            all_results.extend(results)
            print(f"  Matched {len(results)}/{len(batch)}")
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)

        if i + batch_size < len(addresses):
            time.sleep(3)

    # Save results
    with open('geocoded.json', 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2)

    print(f"\nDone. Geocoded {len(all_results)}/{len(addresses)} addresses → geocoded.json")


if __name__ == '__main__':
    main()
