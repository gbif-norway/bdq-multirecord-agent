#!/usr/bin/env python3
"""
Quick test script to verify the column normalization fix works with occurrence.txt
"""

import sys
import os
sys.path.append('.')

from app.services.bdq_cli_service import BDQCLIService
from app.services.csv_service import CSVService

def test_column_normalization_fix():
    """Test that occurrence.txt now gets applicable BDQ tests"""
    
    print("🧪 Testing column normalization fix...")
    
    # Initialize services
    bdq_service = BDQCLIService(skip_validation=True)
    csv_service = CSVService()
    
    # Load occurrence.txt
    occ_txt_path = "tests/data/occurrence.txt"
    with open(occ_txt_path, 'r') as f:
        content = f.read()
    
    df, core_type = csv_service.parse_csv_and_detect_core(content)
    
    print(f"📁 Loaded {occ_txt_path}")
    print(f"   Core type: {core_type}")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    
    # Show some sample columns
    sample_columns = list(df.columns)[:10]
    print(f"   Sample columns: {sample_columns}")
    
    # Get available tests
    tests = bdq_service.get_available_tests()
    print(f"\n📋 Available BDQ tests: {len(tests)}")
    
    # Test filtering with normalization
    applicable_tests = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
    
    print(f"\n✅ Results after normalization:")
    print(f"   Applicable tests: {len(applicable_tests)}")
    
    if applicable_tests:
        print(f"   Success! Found {len(applicable_tests)} applicable tests")
        
        # Show some examples
        print(f"\n📝 Example applicable tests:")
        for i, test in enumerate(applicable_tests[:5]):
            required_cols = test.actedUpon + test.consulted
            required_cols = [col for col in required_cols if col.strip()]
            print(f"   {i+1}. {test.id}")
            print(f"      Requires: {required_cols}")
        
        if len(applicable_tests) > 5:
            print(f"   ... and {len(applicable_tests) - 5} more tests")
            
    else:
        print("   ❌ Still no applicable tests - fix didn't work")
        return False
    
    return True

def test_simple_csv_comparison():
    """Compare results with simple CSV files"""
    
    print("\n🔍 Comparing with simple CSV files...")
    
    bdq_service = BDQCLIService(skip_validation=True)
    csv_service = CSVService()
    
    test_files = [
        ("tests/data/simple_occurrence_dwc.csv", "Simple occurrence (no prefix)"),
        ("tests/data/prefixed_occurrence_dwc.csv", "Prefixed occurrence (dwc:)")
    ]
    
    for file_path, description in test_files:
        with open(file_path, 'r') as f:
            content = f.read()
        
        df, core_type = csv_service.parse_csv_and_detect_core(content)
        tests = bdq_service.get_available_tests()
        applicable = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
        
        print(f"   {description}: {len(applicable)} applicable tests")

if __name__ == "__main__":
    print("🚀 Testing Darwin Core column normalization fix\n")
    
    try:
        # Test the main fix
        success = test_column_normalization_fix()
        
        # Compare with other CSV files
        test_simple_csv_comparison()
        
        print(f"\n{'✅ SUCCESS' if success else '❌ FAILED'}: Column normalization test completed")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()