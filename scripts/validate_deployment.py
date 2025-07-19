#!/usr/bin/env python3
"""
Trading Data Pipeline Deployment Validation Script
Tests all critical components for production deployment.
"""

import sys
import os
import importlib
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_imports():
    """Test critical package imports."""
    logger.info("🔍 Testing critical imports...")
    
    critical_packages = [
        'pandas',
        'numpy',
        'yfinance',
        'google.cloud.bigquery',
        'google.cloud.storage',
        'google.cloud.secretmanager',
        'kiteconnect',
        'yaml',
        'flask',
        'gunicorn',
        'schedule'
    ]
    
    failed_imports = []
    
    for package in critical_packages:
        try:
            importlib.import_module(package)
            logger.info(f"✅ {package}")
        except ImportError as e:
            logger.error(f"❌ {package}: {e}")
            failed_imports.append(package)
    
    # Test project-specific imports
    try:
        sys.path.append('src')
        import trading_data_pipeline
        logger.info("✅ trading_data_pipeline module")
    except ImportError as e:
        logger.error(f"❌ trading_data_pipeline module: {e}")
        failed_imports.append('trading_data_pipeline')
    
    return len(failed_imports) == 0

def test_config_files():
    """Test presence of required configuration files."""
    logger.info("🔍 Testing configuration files...")
    
    required_configs = [
        'config/bq_config.yaml',
        'requirements.txt',
        'setup.py',
        'Dockerfile'
    ]
    
    missing_configs = []
    
    for config in required_configs:
        config_path = Path(config)
        if config_path.exists():
            logger.info(f"✅ {config}")
        else:
            logger.error(f"❌ {config} not found")
            missing_configs.append(config)
    
    return len(missing_configs) == 0

def test_entry_points():
    """Test entry point scripts."""
    logger.info("🔍 Testing entry point scripts...")
    
    entry_points = [
        'bin/batch_upload_historical.py',
        'bin/complete_historical_pipeline.py',
        'bin/daily_data_scheduler.py',
        'bin/enhanced_batch_upload.py',
        'bin/realtime_data_puller.py'
    ]
    
    missing_scripts = []
    
    for script in entry_points:
        script_path = Path(script)
        if script_path.exists():
            logger.info(f"✅ {script}")
        else:
            logger.error(f"❌ {script} not found")
            missing_scripts.append(script)
    
    return len(missing_scripts) == 0

def test_package_installation():
    """Test that the package can be installed."""
    logger.info("🔍 Testing package installation...")
    
    try:
        # Try to import the package after installation
        sys.path.insert(0, 'src')
        import trading_data_pipeline
        logger.info("✅ Package installation successful")
        return True
    except Exception as e:
        logger.error(f"❌ Package installation failed: {e}")
        return False

def test_docker_environment():
    """Test Docker-specific environment variables and setup."""
    logger.info("🔍 Testing Docker environment...")
    
    # Check if we're running in Docker
    if os.path.exists('/.dockerenv'):
        logger.info("✅ Running in Docker container")
        
        # Test user permissions
        try:
            test_file = Path('/tmp/test_write')
            test_file.write_text('test')
            test_file.unlink()
            logger.info("✅ File write permissions")
        except Exception as e:
            logger.error(f"❌ File write permissions: {e}")
            return False
        
        # Test working directory
        if Path.cwd().name == 'app':
            logger.info("✅ Correct working directory")
        else:
            logger.error(f"❌ Wrong working directory: {Path.cwd()}")
            return False
    else:
        logger.info("ℹ️  Not running in Docker (local environment)")
    
    return True

def test_basic_functionality():
    """Test basic functionality of key components."""
    logger.info("🔍 Testing basic functionality...")
    
    try:
        # Test pandas functionality
        import pandas as pd
        df = pd.DataFrame({'test': [1, 2, 3]})
        assert len(df) == 3
        logger.info("✅ Pandas functionality")
        
        # Test numpy functionality
        import numpy as np
        arr = np.array([1, 2, 3])
        assert arr.sum() == 6
        logger.info("✅ NumPy functionality")
        
        # Test yaml loading
        import yaml
        test_yaml = {'test': 'value'}
        yaml_str = yaml.dump(test_yaml)
        loaded = yaml.safe_load(yaml_str)
        assert loaded == test_yaml
        logger.info("✅ YAML functionality")
        
        return True
    except Exception as e:
        logger.error(f"❌ Basic functionality test failed: {e}")
        return False

def main():
    """Run all validation tests."""
    logger.info("🚀 Starting Trading Data Pipeline deployment validation...")
    
    tests = [
        ("Import Tests", test_imports),
        ("Configuration Files", test_config_files),
        ("Entry Points", test_entry_points),
        ("Package Installation", test_package_installation),
        ("Docker Environment", test_docker_environment),
        ("Basic Functionality", test_basic_functionality)
    ]
    
    results = {}
    all_passed = True
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            results[test_name] = test_func()
            if results[test_name]:
                logger.info(f"✅ {test_name} PASSED")
            else:
                logger.error(f"❌ {test_name} FAILED")
                all_passed = False
        except Exception as e:
            logger.error(f"❌ {test_name} FAILED with exception: {e}")
            results[test_name] = False
            all_passed = False
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("VALIDATION SUMMARY")
    logger.info(f"{'='*60}")
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
    
    if all_passed:
        logger.info("\n🎉 ALL TESTS PASSED! Trading Data Pipeline is ready for deployment.")
        sys.exit(0)
    else:
        logger.error("\n💥 SOME TESTS FAILED! Please fix the issues before deployment.")
        sys.exit(1)

if __name__ == "__main__":
    main()
