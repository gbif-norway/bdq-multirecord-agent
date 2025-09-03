#!/usr/bin/env python3

import csv
import os
import sys

def test_tg2_parsing():
    print("=== TG2 CSV Parsing Test ===")
    
    # Check if TG2_tests.csv exists
    tg2_paths = [
        "TG2_tests.csv",
        "bdq-spec/tg2/core/TG2_tests.csv",
        "/app/TG2_tests.csv",
        "/app/bdq-spec/tg2/core/TG2_tests.csv"
    ]
    
    tg2_path = None
    for path in tg2_paths:
        if os.path.exists(path):
            tg2_path = path
            break
    
    if not tg2_path:
        print("❌ ERROR: TG2_tests.csv not found in any expected location!")
        print(f"Checked paths: {tg2_paths}")
        print(f"Current directory: {os.getcwd()}")
        print("Files in current directory:")
        for f in os.listdir('.'):
            if 'TG2' in f or f.endswith('.csv'):
                print(f"  {f}")
        return False
    
    print(f"✅ Found TG2_tests.csv at: {tg2_path}")
    
    # Parse the CSV
    try:
        with open(tg2_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            
        print(f"✅ Successfully parsed CSV: {len(rows)} rows")
        
        # Check headers
        headers = list(rows[0].keys()) if rows else []
        print(f"Headers: {headers}")
        
        required_headers = ['Label', 'InformationElement:ActedUpon', 'InformationElement:Consulted', 
                          'Parameters', 'Link to Specification Source Code']
        missing_headers = [h for h in required_headers if h not in headers]
        if missing_headers:
            print(f"❌ Missing required headers: {missing_headers}")
            return False
        
        print("✅ All required headers present")
        
        # Sample some test parsing
        sample_tests = []
        for i, row in enumerate(rows[:10]):  # First 10 tests
            label = row.get('Label', '').strip()
            acted_upon = row.get('InformationElement:ActedUpon', '').strip()
            consulted = row.get('InformationElement:Consulted', '').strip()
            source_link = row.get('Link to Specification Source Code', '').strip()
            
            if not label:
                continue
                
            # Parse acted upon and consulted
            acted_upon_list = [f.strip() for f in acted_upon.split(',') if f.strip()] if acted_upon else []
            consulted_list = [f.strip() for f in consulted.split(',') if f.strip()] if consulted else []
            
            # Try to extract library from source link
            library = None
            if 'geo_ref_qc' in source_link:
                library = 'geo_ref_qc'
            elif 'event_date_qc' in source_link:
                library = 'event_date_qc'
            elif 'sci_name_qc' in source_link:
                library = 'sci_name_qc'
            elif 'rec_occur_qc' in source_link:
                library = 'rec_occur_qc'
            
            sample_tests.append({
                'label': label,
                'acted_upon': acted_upon_list,
                'consulted': consulted_list,
                'library': library,
                'source_link': source_link
            })
        
        print(f"\n=== Sample Tests ===")
        for test in sample_tests:
            print(f"Test: {test['label']}")
            print(f"  ActedUpon: {test['acted_upon']}")  
            print(f"  Consulted: {test['consulted']}")
            print(f"  Library: {test['library']}")
            print(f"  Source: {test['source_link'][:60]}...")
            print()
        
        print(f"✅ Successfully parsed {len(sample_tests)} sample tests")
        
        # Count tests by library
        library_counts = {}
        for row in rows:
            source_link = row.get('Link to Specification Source Code', '')
            if 'geo_ref_qc' in source_link:
                library_counts['geo_ref_qc'] = library_counts.get('geo_ref_qc', 0) + 1
            elif 'event_date_qc' in source_link:
                library_counts['event_date_qc'] = library_counts.get('event_date_qc', 0) + 1
            elif 'sci_name_qc' in source_link:
                library_counts['sci_name_qc'] = library_counts.get('sci_name_qc', 0) + 1
            elif 'rec_occur_qc' in source_link:
                library_counts['rec_occur_qc'] = library_counts.get('rec_occur_qc', 0) + 1
        
        print(f"=== Test Count by Library ===")
        for lib, count in library_counts.items():
            print(f"  {lib}: {count} tests")
        
        total_mapped = sum(library_counts.values())
        print(f"Total mappable tests: {total_mapped}/{len(rows)}")
        
        if total_mapped == 0:
            print("❌ ERROR: No tests could be mapped to libraries!")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR parsing TG2 CSV: {e}")
        return False

if __name__ == "__main__":
    success = test_tg2_parsing()
    sys.exit(0 if success else 1)