from src.udp_voice_server import run_server


if __name__ == "__main__":
    # Bind on all interfaces for local, emulator (10.0.2.2), and LAN access.
    run_server("0.0.0.0", 50010)


