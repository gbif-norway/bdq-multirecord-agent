#!/usr/bin/env python3
"""
Quick test script to verify the test setup is working correctly.
This script runs a few basic tests to ensure the testing infrastructure is properly configured.
"""

import sys
import os
import subprocess
from pathlib import Path
from unittest.mock import patch

def check_dependencies():
    """Check if required dependencies are available"""
    print("🔍 Checking dependencies...")
    
    try:
        import pytest
        print(f"✅ pytest {pytest.__version__} available")
    except ImportError:
        print("❌ pytest not available")
        return False
    
    try:
        import pandas
        print(f"✅ pandas {pandas.__version__} available")
    except ImportError:
        print("❌ pandas not available")
        return False
    
    try:
        import fastapi
        print(f"✅ fastapi {fastapi.__version__} available")
    except ImportError:
        print("❌ fastapi not available")
        return False
    
    return True

def check_test_structure():
    """Check if test files and structure are correct"""
    print("\n🔍 Checking test structure...")
    
    test_dir = Path("tests")
    if not test_dir.exists():
        print("❌ tests directory not found")
        return False
    
    required_files = [
        "tests/__init__.py",
        "tests/conftest.py",
        "tests/test_main.py",
        "tests/test_bdq_cli_service.py",
        "tests/test_tg2_parser.py",
        "tests/test_csv_service.py",
        "tests/test_email_service.py",
        "tests/test_llm_service.py"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing test files: {missing_files}")
        return False
    
    print("✅ All test files present")
    return True

def check_app_structure():
    """Check if application structure is correct"""
    print("\n🔍 Checking application structure...")
    
    app_dir = Path("app")
    if not app_dir.exists():
        print("❌ app directory not found")
        return False
    
    required_services = [
        "app/services/bdq_cli_service.py",
        "app/services/tg2_parser.py",
        "app/services/csv_service.py",
        "app/services/email_service.py",
        "app/services/llm_service.py"
    ]
    
    missing_services = []
    for service_path in required_services:
        if not Path(service_path).exists():
            missing_services.append(service_path)
    
    if missing_services:
        print(f"❌ Missing service files: {missing_services}")
        return False
    
    print("✅ All service files present")
    return True

def run_simple_test():
    """Run a simple test to verify pytest is working"""
    print("\n🧪 Running simple test...")
    
    try:
        # Create a simple test file
        test_content = '''
import pytest

def test_simple():
    """Simple test to verify pytest is working"""
    assert 1 + 1 == 2
    assert "hello" in "hello world"
    print("✅ Simple test passed!")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
'''
        
        with open("test_simple.py", "w") as f:
            f.write(test_content)
        
        # Run the test
        result = subprocess.run([
            sys.executable, "-m", "pytest", "test_simple.py", "-v"
        ], capture_output=True, text=True)
        
        # Clean up
        os.remove("test_simple.py")
        
        if result.returncode == 0:
            print("✅ pytest execution successful")
            return True
        else:
            print(f"❌ pytest execution failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error running simple test: {e}")
        return False

def run_import_test():
    """Test if all modules can be imported"""
    print("\n🔍 Testing module imports...")
    
    modules_to_test = [
        "app.main",
        "app.services.tg2_parser",
        "app.services.csv_service",
        "app.services.email_service",
        "app.services.llm_service",
        "app.models.email_models"
    ]
    
    failed_imports = []
    for module in modules_to_test:
        try:
            __import__(module)
            print(f"✅ {module} imported successfully")
        except ImportError as e:
            print(f"❌ {module} import failed: {e}")
            failed_imports.append(module)
    
    # Test BDQ CLI service separately with file mocking
    try:
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            __import__("app.services.bdq_cli_service")
            print("✅ app.services.bdq_cli_service imported successfully")
    except Exception as e:
        print(f"❌ app.services.bdq_cli_service import failed: {e}")
        failed_imports.append("app.services.bdq_cli_service")
    
    if failed_imports:
        print(f"❌ Failed imports: {failed_imports}")
        return False
    
    print("✅ All modules imported successfully")
    return True

def main():
    """Main test execution"""
    print("🚀 BDQ Email Report Service - Quick Test")
    print("=" * 50)
    
    checks = [
        ("Dependencies", check_dependencies),
        ("Test Structure", check_test_structure),
        ("App Structure", check_app_structure),
        ("Module Imports", run_import_test),
        ("Pytest Execution", run_simple_test)
    ]
    
    results = []
    for check_name, check_func in checks:
        try:
            result = check_func()
            results.append((check_name, result))
        except Exception as e:
            print(f"❌ {check_name} check failed with exception: {e}")
            results.append((check_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for check_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{check_name:20} {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n🎉 All checks passed! The test setup is working correctly.")
        print("\nYou can now run the full test suite with:")
        print("  python run_tests.py")
        print("  python -m pytest tests/ -v")
        print("  docker-compose -f docker-compose.test.yml --profile test up test-runner")
        return 0
    else:
        print(f"\n⚠️  {total - passed} checks failed. Please review the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
