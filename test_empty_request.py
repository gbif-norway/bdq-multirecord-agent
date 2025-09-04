#!/usr/bin/env python3

import json
import subprocess
import tempfile
import os

def test_empty_request():
    """Test CLI with minimal request to see basic functionality"""
    
    cli_input = {
        "requestId": "empty-test",
        "tests": []
    }
    
    input_json = json.dumps(cli_input, indent=2)
    print(f"Testing with empty request:")
    print(input_json)
    
    # Write to temp files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(input_json)
        input_file = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_file = f.name
    
    cli_jar = './java/bdq-cli/target/bdq-cli-1.0.0.jar'
    
    java_cmd = [
        'java', '-jar', cli_jar,
        f'--input={input_file}',
        f'--output={output_file}'
    ]
    
    print(f"\\nCommand: {' '.join(java_cmd)}")
    
    try:
        result = subprocess.run(
            java_cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        print(f"Return code: {result.returncode}")
        
        if result.stdout:
            print(f"STDOUT: {result.stdout}")
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                output_content = f.read()
            print(f"Output: {output_content}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        try:
            os.unlink(input_file)
            os.unlink(output_file)
        except:
            pass

if __name__ == "__main__":
    test_empty_request()