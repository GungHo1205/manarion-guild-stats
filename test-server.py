#!/usr/bin/env python3
"""
Simple local testing server for guild stats dashboard
Run this in your project directory and visit http://localhost:8000
"""

import http.server
import socketserver
import os
import json
from datetime import datetime

class LocalTestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="docs", **kwargs)
    
    def end_headers(self):
        # Add CORS headers for local testing
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()

def create_test_data():
    """Create sample data for testing"""
    test_data = {
        "lastUpdated": datetime.utcnow().isoformat() + 'Z',
        "baselineDate": "2025-01-15",
        "baselineTimestamp": "2025-01-15T00:00:00Z",
        "guilds": [
            {
                "GuildName": "Test Guild Alpha",
                "StudyLevel": 450,
                "NexusLevel": 320,
                "StudyProgress": 5,
                "NexusProgress": 3,
                "StudyCodexCost": 2275,
                "NexusCodexCost": 966,
                "TotalCodexCost": 3241
            },
            {
                "GuildName": "Test Guild Beta",
                "StudyLevel": 380,
                "NexusLevel": 290,
                "StudyProgress": 2,
                "NexusProgress": 1,
                "StudyCodexCost": 763,
                "NexusCodexCost": 291,
                "TotalCodexCost": 1054
            },
            {
                "GuildName": "Test Guild Gamma",
                "StudyLevel": 520,
                "NexusLevel": 410,
                "StudyProgress": 0,
                "NexusProgress": 0,
                "StudyCodexCost": 0,
                "NexusCodexCost": 0,
                "TotalCodexCost": 0
            },
            {
                "GuildName": "Test Guild Delta",
                "StudyLevel": 275,
                "NexusLevel": 180,
                "StudyProgress": 8,
                "NexusProgress": 5,
                "StudyCodexCost": 2208,
                "NexusCodexCost": 915,
                "TotalCodexCost": 3123
            }
        ]
    }
    
    os.makedirs("docs", exist_ok=True)
    with open("docs/guild-data.json", "w") as f:
        json.dump(test_data, f, indent=2)
    
    print("Created test data in docs/guild-data.json")

if __name__ == "__main__":
    PORT = 8000
    
    if not os.path.exists("docs/guild-data.json"):
        print("No test data found. Creating sample data...")
        create_test_data()
    
    with socketserver.TCPServer(("", PORT), LocalTestHandler) as httpd:
        print(f"🚀 Local test server running at http://localhost:{PORT}")
        print("📁 Serving files from ./docs directory")
        print("🔄 Press Ctrl+C to stop")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n✅ Server stopped")