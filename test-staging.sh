#!/bin/bash
# test-staging-setup.sh
# Script to test the staging setup locally

echo "Testing Staging Setup"
echo "========================"

# Create staging directory and test files
echo " Creating staging directory..."
mkdir -p staging

# Copy docs files to staging
if [ -d "docs" ]; then
    cp docs/* staging/ 2>/dev/null || true
    echo " Copied docs files to staging"
else
    echo " No docs directory found"
    exit 1
fi

# Modify staging index.html
if [ -f "staging/index.html" ]; then
    echo " Adding staging visual indicators..."
    
    # Backup original
    cp staging/index.html staging/index.html.bak
    
    # Add staging title
    sed -i 's/<title>Guild Stats Dashboard<\/title>/<title>[DEV] Guild Stats Dashboard<\/title>/' staging/index.html
    
    # Add staging CSS variables
    sed -i '/--gradient-3:/a \
        --staging-bg: linear-gradient(45deg, #ff6b35, #f7931e); \
        --staging-text: #ffffff; \
        --staging-border: #ff6b35;' staging/index.html
    
    # Add staging banner
    sed -i '/<header class="header">/a \
        <div style="background: var(--staging-bg); margin: -40px -25px 20px -25px; padding: 12px 0; border-radius: 15px 15px 0 0; text-align: center; border: 2px solid var(--staging-border);"> \
          <span style="color: var(--staging-text); font-size: 1rem; font-weight: 700; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">🚧 STAGING ENVIRONMENT - DEV BRANCH 🚧</span> \
        </div>' staging/index.html
    
    echo "Staging modifications applied"
else
    echo " No index.html found in staging directory"
    exit 1
fi

echo ""
echo " Test Results:"
echo "=================="
echo " Staging directory: $(pwd)/staging"
echo " Files created:"
ls -la staging/

echo ""
echo " To test locally:"
echo "1. Run: python test-server.py"
echo "2. Visit: http://localhost:8000 (production version)"
echo "3. Visit: http://localhost:8000/staging/ (staging version)"
echo ""
echo " You should see:"
echo "   - Production: Normal blue header"
echo "   - Staging: Orange banner with '🚧 STAGING ENVIRONMENT' text"