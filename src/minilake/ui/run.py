"""
Run the MiniLake Explorer dashboard.

This script creates and runs the Dash application for MiniLake.
"""

import sys

from minilake import Config
from minilake.ui.app import create_app


def main():
    """Run the MiniLake Explorer dashboard."""
    # Parse command line arguments
    host = "0.0.0.0"
    port = 8050
    debug = False

    # Check for command line arguments
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg.startswith("--host="):
                host = arg.split("=")[1]
            elif arg.startswith("--port="):
                port = int(arg.split("=")[1])
            elif arg == "--debug":
                debug = True

    # Create and run the app with config from environment
    config = Config.from_env()
    app = create_app(config)
    print(f"Starting MiniLake Explorer on http://{host}:{port}")
    app.run_server(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
