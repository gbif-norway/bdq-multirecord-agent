#!/usr/bin/env python3

def test_filtering_logic():
    print("=== Local BDQ Filtering Test ===")
    
    # Get CSV columns from occurrence.txt
    with open('occurrence.txt', 'r') as f:
        header_line = f.readline().strip()
    
    csv_columns = header_line.split('\t')
    csv_columns_lower = [col.lower() for col in csv_columns]
    
    print(f"CSV has {len(csv_columns)} columns")
    print(f"Sample columns: {csv_columns[:10]}...")
    print(f"Key DwC columns present:")
    
    # Check key Darwin Core columns
    key_dwc_columns = ['occurrenceID', 'basisOfRecord', 'country', 'countryCode', 
                      'decimalLatitude', 'decimalLongitude', 'geodeticDatum', 
                      'scientificName', 'year', 'month', 'phylum']
    
    for col in key_dwc_columns:
        present = col in csv_columns
        present_lower = col.lower() in csv_columns_lower
        print(f"  {col}: exact={present}, lowercase={present_lower}")
    
    print(f"\nAll CSV columns (lowercase): {sorted(csv_columns_lower)}")
    
    # Test mapping logic
    dwc_mapping = {
        'dwc:countrycode': ['countrycode', 'country_code'],
        'dwc:country': ['country'],
        'dwc:decimallatitude': ['decimallatitude', 'latitude', 'lat'],
        'dwc:decimallongitude': ['decimallongitude', 'longitude', 'lon'],
        'dwc:geodeticdatum': ['geodeticdatum', 'datum'],
        'dwc:scientificname': ['scientificname', 'scientific_name', 'sciname'],
        'dwc:year': ['year'],
        'dwc:month': ['month'],
        'dwc:basisofrecord': ['basisofrecord', 'basis_of_record', 'basis'],
        'dwc:occurrenceid': ['occurrenceid', 'occurrence_id', 'id'],
        'dwc:phylum': ['phylum'],
    }
    
    print(f"\n=== Testing Column Mappings ===")
    for dwc_term, mapped_cols in dwc_mapping.items():
        matches = [col for col in mapped_cols if col in csv_columns_lower]
        found = len(matches) > 0
        print(f"{dwc_term:25} -> {mapped_cols} -> {found} {matches if matches else ''}")
    
    # Simulate some common BDQ tests
    print(f"\n=== Simulating BDQ Tests ===")
    
    sample_tests = [
        {
            'id': 'VALIDATION_COUNTRY_FOUND',
            'actedUpon': ['dwc:country'],
            'consulted': []
        },
        {
            'id': 'VALIDATION_COORDINATES_NOTZERO', 
            'actedUpon': ['dwc:decimalLatitude', 'dwc:decimalLongitude'],
            'consulted': []
        },
        {
            'id': 'VALIDATION_OCCURRENCEID_NOTEMPTY',
            'actedUpon': ['dwc:occurrenceID'],
            'consulted': []
        },
        {
            'id': 'VALIDATION_SCIENTIFICNAME_FOUND',
            'actedUpon': ['dwc:scientificName'],
            'consulted': []
        },
        {
            'id': 'VALIDATION_BASISOFRECORD_STANDARD',
            'actedUpon': ['dwc:basisOfRecord'], 
            'consulted': []
        }
    ]
    
    applicable_count = 0
    
    for test in sample_tests:
        test_columns = test['actedUpon'] + test['consulted']
        test_columns_lower = [col.lower() for col in test_columns]
        
        print(f"\nTest: {test['id']}")
        print(f"  Needs: {test_columns}")
        
        all_present = True
        missing = []
        
        for test_col in test_columns_lower:
            found = False
            if test_col in dwc_mapping:
                mapped_cols = dwc_mapping[test_col]
                if any(mapped_col in csv_columns_lower for mapped_col in mapped_cols):
                    found = True
                    matches = [col for col in mapped_cols if col in csv_columns_lower]
                    print(f"    {test_col} -> {mapped_cols} -> ✓ {matches}")
                else:
                    print(f"    {test_col} -> {mapped_cols} -> ✗")
                    missing.append(test_col)
            else:
                if test_col in csv_columns_lower:
                    found = True
                    print(f"    {test_col} -> Direct -> ✓")
                else:
                    print(f"    {test_col} -> Direct -> ✗")
                    missing.append(test_col)
            
            if not found:
                all_present = False
        
        if all_present:
            print(f"  Result: ✓ APPLICABLE")
            applicable_count += 1
        else:
            print(f"  Result: ✗ MISSING {missing}")
    
    print(f"\n=== SUMMARY ===")
    print(f"CSV columns: {len(csv_columns)}")
    print(f"Sample tests evaluated: {len(sample_tests)}")  
    print(f"Applicable tests: {applicable_count}")
    
    if applicable_count == 0:
        print("\n❌ PROBLEM: No tests are applicable!")
        print("This explains why you're seeing 'no tests to run'")
    else:
        print(f"\n✅ SUCCESS: {applicable_count} tests should be applicable")

if __name__ == "__main__":
    test_filtering_logic()