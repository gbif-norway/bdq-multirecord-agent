#!/usr/bin/env python3

import pandas as pd

def debug_filtering():
    print("=== BDQ Test Filtering Debug ===")
    
    # Read the occurrence.txt file
    try:
        df = pd.read_csv('occurrence.txt', sep='\t')
        print(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    csv_columns = list(df.columns)
    csv_columns_lower = [col.lower() for col in csv_columns]
    print(f"CSV columns (lowercase): {csv_columns_lower}")
    
    # Darwin Core term to common CSV column mapping
    dwc_mapping = {
        'dwc:countrycode': ['countrycode', 'country_code', 'countrycode'],
        'dwc:country': ['country'],
        'dwc:dateidentified': ['dateidentified', 'date_identified', 'dateidentified'],
        'dwc:phylum': ['phylum'],
        'dwc:minimumdepthinmeters': ['minimumdepthinmeters', 'min_depth', 'mindepth'],
        'dwc:maximumdepthinmeters': ['maximumdepthinmeters', 'max_depth', 'maxdepth'],
        'dwc:decimallatitude': ['decimallatitude', 'latitude', 'lat', 'decimallatitude'],
        'dwc:decimallongitude': ['decimallongitude', 'longitude', 'lon', 'decimallongitude'],
        'dwc:verbatimcoordinates': ['verbatimcoordinates', 'coordinates', 'coords'],
        'dwc:geodeticdatum': ['geodeticdatum', 'datum'],
        'dwc:scientificname': ['scientificname', 'scientific_name', 'sciname'],
        'dwc:year': ['year'],
        'dwc:month': ['month'],
        'dwc:day': ['day'],
        'dwc:eventdate': ['eventdate', 'event_date', 'date'],
        'dwc:basisofrecord': ['basisofrecord', 'basis_of_record', 'basis'],
        'dwc:occurrenceid': ['occurrenceid', 'occurrence_id', 'id'],
        'dwc:taxonid': ['taxonid', 'taxon_id', 'id']
    }
    
    print("\n=== Testing Common Darwin Core Terms ===")
    test_terms = ['dwc:country', 'dwc:countrycode', 'dwc:occurrenceid', 'dwc:basisofrecord', 
                  'dwc:decimallatitude', 'dwc:decimallongitude', 'dwc:scientificname', 
                  'dwc:year', 'dwc:month', 'dwc:geodeticdatum']
    
    for term in test_terms:
        term_lower = term.lower()
        if term_lower in dwc_mapping:
            mapped_cols = dwc_mapping[term_lower]
            present = any(mapped_col in csv_columns_lower for mapped_col in mapped_cols)
            print(f"{term} -> {mapped_cols} -> Present: {present}")
            if present:
                matching = [col for col in mapped_cols if col in csv_columns_lower]
                print(f"  Matching columns: {matching}")
        else:
            # Direct match
            direct_match = term_lower in csv_columns_lower
            print(f"{term} -> Direct match: {direct_match}")
    
    print("\n=== Sample Test Simulation ===")
    # Test a simple validation that should work
    sample_test_columns = ['dwc:country']
    print(f"Sample test requires: {sample_test_columns}")
    
    all_present = True
    for test_col in sample_test_columns:
        test_col_lower = test_col.lower()
        if test_col_lower in dwc_mapping:
            mapped_cols = dwc_mapping[test_col_lower]
            col_present = any(mapped_col in csv_columns_lower for mapped_col in mapped_cols)
            print(f"  {test_col} -> {mapped_cols} -> {col_present}")
            if not col_present:
                all_present = False
        else:
            direct_present = test_col_lower in csv_columns_lower
            print(f"  {test_col} -> Direct: {direct_present}")
            if not direct_present:
                all_present = False
    
    print(f"Sample test would be applicable: {all_present}")

if __name__ == "__main__":
    debug_filtering()