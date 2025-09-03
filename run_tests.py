#!/usr/bin/env python3
"""
Test runner script for BDQ Email Report Service

This script provides an easy way to run tests with different configurations.
"""

import sys
import subprocess
import argparse
from pathlib import Path


def run_tests(test_type="all", coverage=False, verbose=False, parallel=False):
    """Run tests with the specified configuration"""
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add test type filters
    if test_type == "unit":
        cmd.extend(["-m", "unit"])
    elif test_type == "integration":
        cmd.extend(["-m", "integration"])
    elif test_type == "fast":
        cmd.extend(["-m", "not slow"])
    
    # Add coverage if requested
    if coverage:
        cmd.extend(["--cov=app", "--cov-report=html", "--cov-report=term-missing"])
    
    # Add verbosity
    if verbose:
        cmd.extend(["-v", "-s"])
    
    # Add parallel execution
    if parallel:
        cmd.extend(["-n", "auto"])
    
    # Add test discovery
    cmd.append("tests/")
    
    print(f"Running tests with command: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Run BDQ Email Report Service tests")
    parser.add_argument(
        "--type", 
        choices=["all", "unit", "integration", "fast"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    parser.add_argument(
        "--coverage", 
        action="store_true",
        help="Generate coverage report"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--parallel", "-p",
        action="store_true",
        help="Run tests in parallel"
    )
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install test dependencies before running tests"
    )
    
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not Path("app").exists():
        print("Error: 'app' directory not found. Please run this script from the project root.")
        return 1
    
    # Install dependencies if requested
    if args.install_deps:
        print("Installing test dependencies...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
            print("Dependencies installed successfully!")
        except subprocess.CalledProcessError as e:
            print(f"Error installing dependencies: {e}")
            return 1
    
    # Run tests
    exit_code = run_tests(
        test_type=args.type,
        coverage=args.coverage,
        verbose=args.verbose,
        parallel=args.parallel
    )
    
    if exit_code == 0:
        print("\n✅ All tests passed!")
    else:
        print(f"\n❌ Tests failed with exit code {exit_code}")
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
