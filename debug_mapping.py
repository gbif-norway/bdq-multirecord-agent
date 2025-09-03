#!/usr/bin/env python3

def debug_mapping():
    print("=== Darwin Core Mapping Debug ===")
    
    # Test data
    csv_columns = ['occurrenceID', 'country', 'countryCode', 'dateIdentified']
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
    
    print("\nMapping test:")
    for test_col in ['dwc:countrycode', 'dwc:country', 'dwc:dateidentified']:
        test_col_lower = test_col.lower()
        print(f"- {test_col} -> {test_col_lower} -> in mapping: {test_col_lower in dwc_mapping}")
        if test_col_lower in dwc_mapping:
            mapped_cols = dwc_mapping[test_col_lower]
            print(f"  Mapped to: {mapped_cols}")
            present = any(mapped_col in csv_columns_lower for mapped_col in mapped_cols)
            print(f"  Any present in CSV: {present}")

if __name__ == "__main__":
    debug_mapping()
