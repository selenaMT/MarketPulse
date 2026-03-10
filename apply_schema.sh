#!/bin/bash
# Script to apply database schema to Supabase
# Run this from the project root

echo "Applying database schema to Supabase..."
echo "Make sure your DATABASE_URL is set in .env"

# Extract database URL and run schema
python3 -c "
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path('.') / '.env')
db_url = os.getenv('DATABASE_URL')

if not db_url:
    print('DATABASE_URL not found in .env')
    exit(1)

# Convert to psycopg2 format if needed
if db_url.startswith('postgresql://'):
    db_url = db_url.replace('postgresql://', 'postgresql+psycopg2://', 1)

print('Database URL found. Please run the following SQL in your Supabase SQL editor:')
print()
print('=== COPY AND RUN THIS IN SUPABASE SQL EDITOR ===')
with open('database/schema.sql', 'r') as f:
    print(f.read())
print('=== END SQL ===')
"
