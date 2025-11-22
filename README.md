# Refund Tracker

A Flask-based web application for tracking refunds across multiple payment sources.

## Features

- Track refunds from multiple payment methods (Bkash, Nagad, UPay, EBL, SSL, MTB, BRAC, CityBank, SEBL)
- Search by order number, phone number, or seller order number
- View refund details grouped by source
- Background data synchronization from Google Sheets

## Requirements

- Python 3.7+
- pip
- Ubuntu server (or any Linux distribution)

## Installation on Ubuntu Server

### 1. Install Python and dependencies

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv -y
```

### 2. Clone the repository

```bash
git clone https://github.com/promaalfonsa/rtrack.git
cd rtrack
```

### 3. Create a virtual environment (recommended)

```bash
python3 -m venv venv
source venv/bin/activate
```

### 4. Install Python packages

```bash
pip install -r requirements.txt
```

### 5. Configure environment variables (optional)

Create a `.env` file or set environment variables:

```bash
export DATA_DIR="/var/lib/rtrack/data"  # Directory for cached data
export CACHE_TTL="600"                   # Cache time-to-live in seconds
export DISABLE_BACKGROUND_SYNC="1"       # Set to "0" to enable background sync
export LOG_LEVEL="INFO"                  # Logging level (DEBUG, INFO, WARNING, ERROR)
export PORT="5000"                       # Port to run the server on
```

### 6. Run the application

#### Development mode:

```bash
python3 app.py
```

The app will be available at `http://your-server-ip:5000`

#### Production mode with Gunicorn:

For production deployment, use Gunicorn (already specified in Procfile):

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

Or using the Procfile:

```bash
web: gunicorn -w 4 -b 0.0.0.0:$PORT app:app
```

### 7. Run as a system service (recommended for production)

Create a systemd service file:

```bash
sudo nano /etc/systemd/system/rtrack.service
```

Add the following content:

```ini
[Unit]
Description=Refund Tracker Flask App
After=network.target

[Service]
User=www-data
WorkingDirectory=/path/to/rtrack
Environment="PATH=/path/to/rtrack/venv/bin"
Environment="DATA_DIR=/var/lib/rtrack/data"
ExecStart=/path/to/rtrack/venv/bin/gunicorn -w 4 -b 0.0.0.0:5000 app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable rtrack
sudo systemctl start rtrack
sudo systemctl status rtrack
```

### 8. Set up Nginx reverse proxy (optional but recommended)

Install Nginx:

```bash
sudo apt install nginx -y
```

Create an Nginx configuration:

```bash
sudo nano /etc/nginx/sites-available/rtrack
```

Add:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable the site:

```bash
sudo ln -s /etc/nginx/sites-available/rtrack /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## Data Management

The application fetches data from Google Sheets sources defined in `SOURCES` dictionary in `app.py`. 

To manually update the data cache, you can run:

```bash
python3 scripts/fetch_data.py
```

This will download the latest data from all sources and save them as JSON files in the `data/` directory.

## Project Structure

```
rtrack/
├── app.py              # Main Flask application
├── requirements.txt    # Python dependencies
├── Procfile           # Process file for Gunicorn
├── .gitignore         # Git ignore patterns
├── data/              # Cached data files (JSON)
├── scripts/           # Utility scripts
│   └── fetch_data.py  # Script to fetch data from sources
└── templates/         # HTML templates
    ├── index.html
    ├── results.html
    └── sources.html
```

## Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATA_DIR` | `/tmp/data` | Directory for cached data files |
| `CACHE_TTL` | `600` | Cache time-to-live in seconds |
| `REQUEST_TIMEOUT` | `8` | HTTP request timeout in seconds |
| `DISABLE_BACKGROUND_SYNC` | `1` | Disable background data sync (1=disabled, 0=enabled) |
| `REFRESH_INTERVAL` | `600` | Background sync interval in seconds |
| `PORT` | `5000` | Port to run the Flask server |
| `LOG_LEVEL` | `INFO` | Logging level |
| `FLASK_DEBUG` | `0` | Enable Flask debug mode (0=disabled, 1=enabled) |

## License

[Add your license here]

## Support

For issues and questions, please open an issue on GitHub.
