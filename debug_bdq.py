#!/usr/bin/env python3

from app.services.bdq_cli_service import BDQCLIService
from app.services.tg2_parser import TG2Parser

def debug_bdq_tests():
    print("=== BDQ Test Debug ===")
    
    # Parse TG2 tests
    parser = TG2Parser()
    mappings = parser.parse()
    print(f"Total TG2 mappings: {len(mappings)}")
    
    # Create service
    service = BDQCLIService(skip_validation=True)
    service.test_mappings = mappings
    
    # Get available tests
    tests = service.get_available_tests()
    print(f"Total available tests: {len(tests)}")
    
    # Show sample tests
    print("\nSample tests:")
    for t in tests[:5]:
        print(f"- {t.id}: actedUpon={t.actedUpon}, consulted={t.consulted}")
    
    # Test filtering
    csv_columns = ['occurrenceID', 'country', 'countryCode', 'dateIdentified']
    print(f"\nCSV columns: {csv_columns}")
    
    applicable = service.filter_applicable_tests(tests, csv_columns)
    print(f"Applicable tests: {len(applicable)}")
    
    # Debug why tests aren't matching
    print("\nDebug filtering logic:")
    csv_cols_lower = [col.lower() for col in csv_columns]
    print(f"CSV columns (lowercase): {csv_cols_lower}")
    
    for t in tests[:5]:
        test_cols = t.actedUpon + t.consulted
        test_cols_lower = [col.lower() for col in test_cols]
        all_present = all(col in csv_cols_lower for col in test_cols_lower)
        print(f"- {t.id}: test columns {test_cols_lower}, all present: {all_present}")

if __name__ == "__main__":
    debug_bdq_tests()
