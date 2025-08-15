#!/usr/bin/env python3
"""
Windows-compatible script to test staging setup
Run: python test-staging-setup.py
"""

import os
import shutil
import sys

def safe_print(message):
    """Print with Unicode fallback for Windows"""
    try:
        print(message)
    except UnicodeEncodeError:
        # Fallback for Windows cmd prompt
        message = message.replace('🧪', '[TEST]')
        message = message.replace('📁', '[FOLDER]')
        message = message.replace('📄', '[FILE]')
        message = message.replace('✅', '[OK]')
        message = message.replace('⚠️', '[WARNING]')
        message = message.replace('❌', '[ERROR]')
        message = message.replace('🎨', '[STYLE]')
        message = message.replace('🚀', '[ROCKET]')
        message = message.replace('🌐', '[WEB]')
        message = message.replace('🔍', '[SEARCH]')
        message = message.replace('🚧', '[CONSTRUCTION]')
        print(message)

def main():
    safe_print("[TEST] Testing Staging Setup")
    safe_print("========================")
    
    # Create staging directory
    safe_print("[FOLDER] Creating staging directory...")
    if not os.path.exists("staging"):
        os.makedirs("staging")
    
    # Copy docs files to staging
    if os.path.exists("docs"):
        try:
            # Copy each file individually
            for filename in os.listdir("docs"):
                src = os.path.join("docs", filename)
                dst = os.path.join("staging", filename)
                if os.path.isfile(src):
                    shutil.copy2(src, dst)
            safe_print("[OK] Copied docs files to staging")
        except Exception as e:
            safe_print(f"[WARNING] Error copying files: {e}")
    else:
        safe_print("[WARNING] No docs directory found")
        return False
    
    # Modify staging index.html
    index_path = os.path.join("staging", "index.html")
    if os.path.exists(index_path):
        safe_print("[STYLE] Adding staging visual indicators...")
        
        # Backup original
        shutil.copy2(index_path, index_path + ".bak")
        
        try:
            # Read the file
            with open(index_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Make modifications
            content = content.replace(
                '<title>Guild Stats Dashboard</title>',
                '<title>[DEV] Guild Stats Dashboard</title>'
            )
            
            # Add staging CSS variables
            css_addition = '''        --staging-bg: linear-gradient(45deg, #ff6b35, #f7931e);
        --staging-text: #ffffff;
        --staging-border: #ff6b35;'''
            
            content = content.replace(
                '--gradient-3: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);',
                '--gradient-3: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);\n' + css_addition
            )
            
            # Add staging banner
            banner_html = '''        <div style="background: var(--staging-bg); margin: -40px -25px 20px -25px; padding: 12px 0; border-radius: 15px 15px 0 0; text-align: center; border: 2px solid var(--staging-border);">
          <span style="color: var(--staging-text); font-size: 1rem; font-weight: 700; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">[CONSTRUCTION] STAGING ENVIRONMENT - DEV BRANCH [CONSTRUCTION]</span>
        </div>'''
            
            content = content.replace(
                '<header class="header">',
                '<header class="header">\n' + banner_html
            )
            
            # Write the modified file
            with open(index_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            safe_print("[OK] Staging modifications applied")
            
        except Exception as e:
            safe_print(f"[ERROR] Error modifying index.html: {e}")
            return False
    else:
        safe_print("[ERROR] No index.html found in staging directory")
        return False
    
    safe_print("")
    safe_print("[ROCKET] Test Results:")
    safe_print("==================")
    safe_print(f"[FOLDER] Staging directory: {os.path.abspath('staging')}")
    safe_print("[FILE] Files created:")
    
    try:
        for filename in os.listdir("staging"):
            file_path = os.path.join("staging", filename)
            if os.path.isfile(file_path):
                size = os.path.getsize(file_path)
                safe_print(f"  - {filename} ({size} bytes)")
    except Exception as e:
        safe_print(f"[WARNING] Error listing files: {e}")
    
    safe_print("")
    safe_print("[WEB] To test locally:")
    safe_print("1. Run: python test-server.py")
    safe_print("2. Visit: http://localhost:8000 (production version)")
    safe_print("3. Visit: http://localhost:8000/staging/ (staging version)")
    safe_print("")
    safe_print("[SEARCH] You should see:")
    safe_print("   - Production: Normal blue header")
    safe_print("   - Staging: Orange banner with '[CONSTRUCTION] STAGING ENVIRONMENT' text")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)