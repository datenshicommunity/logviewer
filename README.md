# Log Viewer

A real-time log viewer web application built with Go, featuring WebSocket support for live updates and a modern web interface.

## Features

- Real-time log file monitoring
- Support for both `.log` and `.leaks` files
- Basic authentication for secure access
- Modern, responsive web interface
- Memory leak log highlighting
- Buffered WebSocket updates for better performance

## Configuration

The application can be configured using environment variables:

- `LOG_VIEWER_USERNAME`: Username for basic authentication (default: "admin")
- `LOG_VIEWER_PASSWORD`: Password for basic authentication (default: "password123")

## Usage

### Running with Go

```bash
go run main.go
```

The application will start on `http://localhost:8080`.

### Running with Docker

```bash
docker build -t logviewer .
docker run -p 8080:8080 \
    -e LOG_VIEWER_USERNAME=your_username \
    -e LOG_VIEWER_PASSWORD=your_password \
    -v /path/to/logs:/root/ragnarok/log:ro \
    logviewer
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

ochi@datenshi.pw