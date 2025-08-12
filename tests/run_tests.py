import sys
from pathlib import Path

# Ensure project root on sys.path for imports like server.src
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.test_udp_server import test_basic_relay


if __name__ == "__main__":
    test_basic_relay()


