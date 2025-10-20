#!/bin/bash

# Navigate to the directory where the script is located (optional safety)
cd "$(dirname "$0")"

# Relative path to the actual database
TARGET="../../../nlp_news_summary_data/database/scraped_posts.db"

# Link name in the current directory
LINK_NAME="scraped_posts.db"

# Create the symbolic link
ln -sf "$TARGET" "$LINK_NAME"

echo "Symlink created: $LINK_NAME → $TARGET"