#!/bin/bash
OUTPUT="snapshots/labeler_snapshot_$(date +%Y%m%d_%H%M%S).md"

echo "# Perl Compiler Snapshot - $(date)" > "$OUTPUT"
echo "" >> "$OUTPUT"

# Project structure
echo "## Project Structure" >> "$OUTPUT"
echo '```' >> "$OUTPUT"
find . -type f \( -name "*.go" -o -name "*.pl" -o -name "*.sh" \) | grep -v vendor | sort >> "$OUTPUT"
echo '```' >> "$OUTPUT"
echo "" >> "$OUTPUT"

# All Go files
for f in $(find . -name "*.go" | grep -v vendor | sort); do
    echo "## File: $f" >> "$OUTPUT"
    echo '```go' >> "$OUTPUT"
    cat "$f" >> "$OUTPUT"
    echo '```' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

# Test files
for f in $(find . -name "*.pl" | sort); do
    echo "## File: $f" >> "$OUTPUT"
    echo '```perl' >> "$OUTPUT"
    cat "$f" >> "$OUTPUT"
    echo '```' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

# Shell scripts
for f in $(find . -name "*.sh" | sort); do
    echo "## File: $f" >> "$OUTPUT"
    echo '```bash' >> "$OUTPUT"
    cat "$f" >> "$OUTPUT"
    echo '```' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

echo "Snapshot created: $OUTPUT"
wc -l "$OUTPUT"