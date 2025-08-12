VoIP minimal system (Windows server + Android client)

Server (Windows PC):
- Python 3.10+
- UDP relay at port 50010 by default
- Logging configured via `server/src/logger_config.py`

Run:
```powershell
py -3 server/run_server.py
```

Tests:
```powershell
py -3 tests/run_tests.py
```

Router/NAT:
- Forward UDP port 50010 to 192.168.1.67

Android client build:
- Open `android-client/` in Android Studio (Giraffe+), build APK
- Configure IP `192.168.1.67` and port `50010` in app settings UI


