#!/bin/bash
# Benchmark script for p3-all-genomes
# Compares cursor vs offset pagination and alpha vs production servers

set -e

# Configuration
LIMIT=10000
BINARY="${1:-./p3-all-genomes}"
OUTPUT_DIR="${2:-/tmp/p3-benchmark}"
ATTRS="genome_id,genome_name,genome_status"

ALPHA_URL="https://alpha.bv-brc.org/api"
PROD_URL="https://www.bv-brc.org/api"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "============================================"
echo "p3-all-genomes Benchmark"
echo "============================================"
echo "Records per test: $LIMIT"
echo "Binary: $BINARY"
echo "Output directory: $OUTPUT_DIR"
echo "Attributes: $ATTRS"
echo ""

# Check binary exists
if [[ ! -x "$BINARY" ]]; then
    echo "Error: Binary not found or not executable: $BINARY"
    echo "Usage: $0 [path-to-p3-all-genomes] [output-dir]"
    exit 1
fi

# Function to run a benchmark
run_benchmark() {
    local name="$1"
    local api_url="$2"
    local use_cursor="$3"
    local extra_args="$4"
    local output_file="$OUTPUT_DIR/${name}.txt"
    local time_file="$OUTPUT_DIR/${name}.time"

    echo "--------------------------------------------"
    echo "Test: $name"
    echo "API: $api_url"
    echo "Cursor: $use_cursor"
    if [[ -n "$extra_args" ]]; then
        echo "Extra args: $extra_args"
    fi
    echo ""

    # Build command
    local cmd="$BINARY --api-url $api_url --limit $LIMIT --attr $ATTRS --verbose"
    if [[ "$use_cursor" == "yes" ]]; then
        cmd="$cmd --cursor"
    fi
    if [[ -n "$extra_args" ]]; then
        cmd="$cmd $extra_args"
    fi

    echo "Command: $cmd"
    echo ""

    # Run with timing
    local start_time=$(date +%s.%N)

    if $cmd > "$output_file" 2>"$OUTPUT_DIR/${name}.stderr"; then
        local end_time=$(date +%s.%N)
        local elapsed=$(echo "$end_time - $start_time" | bc)
        local line_count=$(wc -l < "$output_file")
        local file_size=$(du -h "$output_file" | cut -f1)

        echo "Status: SUCCESS"
        echo "Time: ${elapsed}s"
        echo "Lines: $line_count (including header)"
        echo "File size: $file_size"

        # Calculate records per second
        local records=$((line_count - 1))  # subtract header
        local rate=$(echo "scale=2; $records / $elapsed" | bc)
        echo "Rate: $rate records/sec"

        # Save timing info
        echo "$name,$elapsed,$records,$rate" >> "$OUTPUT_DIR/results.csv"
    else
        local end_time=$(date +%s.%N)
        local elapsed=$(echo "$end_time - $start_time" | bc)

        echo "Status: FAILED after ${elapsed}s"
        echo "Error output:"
        cat "$OUTPUT_DIR/${name}.stderr" | head -20

        # Save failure info
        echo "$name,$elapsed,FAILED,0" >> "$OUTPUT_DIR/results.csv"
    fi

    echo ""
}

# Initialize results CSV
echo "test,time_seconds,records,records_per_sec" > "$OUTPUT_DIR/results.csv"

echo "Starting benchmarks at $(date)"
echo ""

# Run benchmarks
run_benchmark "alpha_cursor" "$ALPHA_URL" "yes" ""
run_benchmark "alpha_offset" "$ALPHA_URL" "no" ""
run_benchmark "alpha_offset_sorted" "$ALPHA_URL" "no" "--sort genome_id"
run_benchmark "prod_offset" "$PROD_URL" "no" ""
run_benchmark "prod_offset_sorted" "$PROD_URL" "no" "--sort genome_id"

echo "============================================"
echo "Benchmark Summary"
echo "============================================"
echo ""
cat "$OUTPUT_DIR/results.csv" | column -t -s','
echo ""
echo "Detailed results saved to: $OUTPUT_DIR"
echo "Completed at $(date)"
