#!/bin/bash

#==============================================================================
# CONFIGURATION - Edit these variables
#==============================================================================
PROGRAM_PATH="./proxy-grpc-testv4"           # Full path to your program
PROGRAM_ARGS=""                                 # Arguments for your program (optional)
RESTART_INTERVAL=1200                             # Restart interval in MINUTES

#==============================================================================
# Optional settings
#==============================================================================
GRACEFUL_TIMEOUT=5                              # Seconds to wait for graceful shutdown
LOG_ENABLED=false                                # Set to false to disable logging
CHECK_INTERVAL=5                                # Check if program is running every X seconds

# Output handling options:
# "terminal" - Show program output in terminal (default)
# "file"     - Save program output to log file
# "both"     - Show in terminal AND save to file
# "silent"   - Suppress all program output
OUTPUT_MODE="terminal"
LOG_FILE="program.log"                          # Used when OUTPUT_MODE is "file" or "both"

#==============================================================================
# Script logic - Don't edit below unless you know what you're doing
#==============================================================================

# Colors for output
COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_YELLOW='\033[1;33m'
COLOR_RESET='\033[0m'

# Store the PID of the running program
PROGRAM_PID=""

# Logging function
log() {
    if [ "$LOG_ENABLED" = true ]; then
        echo -e "${COLOR_GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${COLOR_RESET} $1"
    fi
}

log_error() {
    echo -e "${COLOR_RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${COLOR_RESET} $1" >&2
}

log_warning() {
    echo -e "${COLOR_YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${COLOR_RESET} $1"
}

# Validate configuration
validate_config() {
    if [ ! -f "$PROGRAM_PATH" ]; then
        log_error "Program not found: $PROGRAM_PATH"
        exit 1
    fi
    
    if [ ! -x "$PROGRAM_PATH" ]; then
        log_error "Program is not executable: $PROGRAM_PATH"
        exit 1
    fi
    
    if ! [[ "$RESTART_INTERVAL" =~ ^[0-9]+$ ]] || [ "$RESTART_INTERVAL" -lt 1 ]; then
        log_error "Invalid restart interval: $RESTART_INTERVAL (must be a positive integer)"
        exit 1
    fi
}

# Start the program
start_program() {
    log "Starting program: $PROGRAM_PATH $PROGRAM_ARGS"
    
    case "$OUTPUT_MODE" in
        "terminal")
            # Show output in terminal (default behavior)
            if [ -n "$PROGRAM_ARGS" ]; then
                $PROGRAM_PATH $PROGRAM_ARGS &
            else
                $PROGRAM_PATH &
            fi
            ;;
        "file")
            # Redirect all output to log file
            if [ -n "$PROGRAM_ARGS" ]; then
                $PROGRAM_PATH $PROGRAM_ARGS >> "$LOG_FILE" 2>&1 &
            else
                $PROGRAM_PATH >> "$LOG_FILE" 2>&1 &
            fi
            ;;
        "both")
            # Show in terminal AND save to file using tee
            if [ -n "$PROGRAM_ARGS" ]; then
                $PROGRAM_PATH $PROGRAM_ARGS 2>&1 | tee -a "$LOG_FILE" &
            else
                $PROGRAM_PATH 2>&1 | tee -a "$LOG_FILE" &
            fi
            ;;
        "silent")
            # Suppress all output
            if [ -n "$PROGRAM_ARGS" ]; then
                $PROGRAM_PATH $PROGRAM_ARGS > /dev/null 2>&1 &
            else
                $PROGRAM_PATH > /dev/null 2>&1 &
            fi
            ;;
        *)
            log_error "Invalid OUTPUT_MODE: $OUTPUT_MODE"
            exit 1
            ;;
    esac
    
    PROGRAM_PID=$!
    log "Program started with PID: $PROGRAM_PID (output: $OUTPUT_MODE)"
}

# Kill the program gracefully
kill_program() {
    if [ -z "$PROGRAM_PID" ]; then
        log_warning "No PID to kill"
        return
    fi
    
    # Check if process is still running
    if ! kill -0 $PROGRAM_PID 2>/dev/null; then
        log_warning "Process $PROGRAM_PID is not running"
        PROGRAM_PID=""
        return
    fi
    
    log "Stopping program (PID: $PROGRAM_PID)..."
    
    # Try graceful shutdown with SIGTERM
    kill -15 $PROGRAM_PID 2>/dev/null || true
    
    # Wait for graceful shutdown
    local waited=0
    while [ $waited -lt $GRACEFUL_TIMEOUT ]; do
        if ! kill -0 $PROGRAM_PID 2>/dev/null; then
            log "Program stopped gracefully"
            PROGRAM_PID=""
            return
        fi
        sleep 1
        waited=$((waited + 1))
    done
    
    # Force kill if still running
    if kill -0 $PROGRAM_PID 2>/dev/null; then
        log_warning "Force killing program (PID: $PROGRAM_PID)"
        kill -9 $PROGRAM_PID 2>/dev/null || true
        sleep 1
    fi
    
    PROGRAM_PID=""
    log "Program stopped"
}

# Cleanup and exit handler (for Ctrl+C)
cleanup_and_exit() {
    echo ""
    log "Received interrupt signal, cleaning up..."
    kill_program
    log "Exiting"
    exit 0
}

# Set up signal handlers
trap cleanup_and_exit SIGINT SIGTERM SIGHUP

# Check if program is still running
is_program_running() {
    if [ -z "$PROGRAM_PID" ]; then
        return 1
    fi
    
    if kill -0 $PROGRAM_PID 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Main loop
main() {
    log "=========================================="
    log "Auto-Restart Script Started"
    log "Program: $PROGRAM_PATH"
    log "Restart Interval: $RESTART_INTERVAL minutes"
    log "Crash Detection: Enabled (checking every $CHECK_INTERVAL seconds)"
    log "Output Mode: $OUTPUT_MODE"
    log "=========================================="
    
    validate_config
    
    while true; do
        start_program
        
        log "Next scheduled restart in $RESTART_INTERVAL minutes (Press Ctrl+C to stop)"
        
        # Calculate total seconds to wait
        local total_seconds=$((RESTART_INTERVAL * 60))
        local elapsed_seconds=0
        
        # Sleep in small intervals and check if program is still running
        while [ $elapsed_seconds -lt $total_seconds ]; do
            sleep $CHECK_INTERVAL
            elapsed_seconds=$((elapsed_seconds + CHECK_INTERVAL))
            
            # Check if program crashed
            if ! is_program_running; then
                log_error "Program crashed or stopped unexpectedly! (PID: $PROGRAM_PID)"
                log "Auto-restarting in 2 seconds..."
                PROGRAM_PID=""
                sleep 2
                break
            fi
        done
        
        # If we completed the full interval, do scheduled restart
        if [ $elapsed_seconds -ge $total_seconds ]; then
            log "Scheduled restart interval reached"
            kill_program
            sleep 2
        fi
    done
}

# Run main function
main
