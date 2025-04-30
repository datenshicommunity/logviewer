package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hpcloud/tail"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		ReadBufferSize:  1024 * 1024, // 1MB
		WriteBufferSize: 1024 * 1024, // 1MB
	}

	activeTails     = make(map[string]*tail.Tail)
	activeTailsLock sync.Mutex

	// Basic auth credentials from environment variables
	username = getEnvOrDefault("LOG_VIEWER_USERNAME", "admin")
	password = getEnvOrDefault("LOG_VIEWER_PASSWORD", "admin")
)

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Failed to upgrade connection:", err)
		return
	}
	defer conn.Close()

	// Set WebSocket connection properties
	conn.SetReadLimit(1024 * 1024) // 1MB max message size
	conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Start ping-pong routine
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered from panic in ping routine:", r)
			}
		}()

		for {
			select {
			case <-pingTicker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	filePath := r.URL.Query().Get("file")
	if filePath == "" {
		return
	}

	// Channel to signal goroutine termination
	done := make(chan struct{})
	defer close(done)

	// Stop any existing tail for this file
	activeTailsLock.Lock()
	if existingTail, exists := activeTails[filePath]; exists {
		existingTail.Stop()
		delete(activeTails, filePath)
	}
	activeTailsLock.Unlock()

	// First, read the entire file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filePath, err)
		return
	}

	lines := strings.Split(string(content), "\n")
	// Send initial content in chunks to avoid large messages
	chunkSize := 100
	for i := 0; i < len(lines); i += chunkSize {
		end := i + chunkSize
		if end > len(lines) {
			end = len(lines)
		}

		var chunk []string
		for _, line := range lines[i:end] {
			if line != "" {
				chunk = append(chunk, line)
			}
		}

		if len(chunk) > 0 {
			message := strings.Join(chunk, "\n")
			if writeErr := conn.WriteMessage(websocket.TextMessage, []byte(message)); writeErr != nil {
				fmt.Printf("Error sending chunk: %v\n", writeErr)
				return
			}
			// Small delay to prevent overwhelming the client
			time.Sleep(10 * time.Millisecond)
		}
	}

	config := tail.Config{
		Follow:    true,
		ReOpen:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END},
		MustExist: true,
		Poll:      true, // Use polling instead of inotify
	}

	tailFile, tailErr := tail.TailFile(filePath, config)
	if tailErr != nil {
		fmt.Printf("Failed to tail file %s: %v\n", filePath, tailErr)
		return
	}

	// Add new tail to active tails
	activeTailsLock.Lock()
	activeTails[filePath] = tailFile
	activeTailsLock.Unlock()

	// Ensure cleanup when connection ends
	defer func() {
		activeTailsLock.Lock()
		if tf, ok := activeTails[filePath]; ok {
			tf.Stop()
			delete(activeTails, filePath)
		}
		activeTailsLock.Unlock()
	}()

	// Buffer for collecting new lines
	var buffer []string
	bufferTimer := time.NewTicker(50 * time.Millisecond)
	defer bufferTimer.Stop()

	// Read from tail file with panic recovery
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic in tail routine: %v\n", r)
		}
	}()

	for {
		select {
		case <-done:
			return
		case line := <-tailFile.Lines:
			if line != nil && line.Text != "" {
				buffer = append(buffer, line.Text)
				// Send immediately if buffer is getting large
				if len(buffer) >= 100 {
					message := strings.Join(buffer, "\n")
					if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
						fmt.Printf("Error sending buffer: %v\n", err)
						return
					}
					buffer = buffer[:0]
				}
			}
		case <-bufferTimer.C:
			if len(buffer) > 0 {
				message := strings.Join(buffer, "\n")
				if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
					fmt.Printf("Error sending buffer: %v\n", err)
					return
				}
				buffer = buffer[:0]
			}
		}
	}
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

type LogFile struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

func basicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted")`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
		if err != nil {
			http.Error(w, "Invalid authorization header", http.StatusBadRequest)
			return
		}

		pair := strings.SplitN(string(payload), ":", 2)
		if len(pair) != 2 || pair[0] != username || pair[1] != password {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func main() {
	http.HandleFunc("/", basicAuth(handleHome))
	http.HandleFunc("/ws", basicAuth(handleWebSocket))
	http.HandleFunc("/logs", basicAuth(handleGetLogs))

	fmt.Println("Server starting at http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.New("home").Parse(`
	<!DOCTYPE html>
	<html>
	<head>
		<title>Log Viewer</title>
		<style>
			body {
				font-family: Arial, sans-serif;
				margin: 0;
				padding: 20px;
				background: #f5f5f5;
			}
			.container {
				display: flex;
				gap: 20px;
			}
			.sidebar {
				width: 300px;
				background: white;
				padding: 20px;
				border-radius: 8px;
				box-shadow: 0 2px 4px rgba(0,0,0,0.1);
			}
			.log-viewer {
				flex-grow: 1;
				background: white;
				padding: 20px;
				border-radius: 8px;
				box-shadow: 0 2px 4px rgba(0,0,0,0.1);
			}
			.log-list {
				list-style: none;
				padding: 0;
				margin: 0;
			}
			.log-item {
				padding: 10px;
				cursor: pointer;
				border-radius: 4px;
				margin-bottom: 5px;
			}
			.log-item:hover {
				background: #f0f0f0;
			}
			.log-content {
				height: 600px;
				overflow-y: auto;
				background: #1e1e1e;
				color: #d4d4d4;
				padding: 10px;
				font-family: monospace;
				white-space: pre-wrap;
				border-radius: 4px;
			}
			.selected {
				background: #e0e0e0;
			}
			.leak-line {
				color: #ff6b6b;
				font-weight: bold;
			}
		</style>
	</head>
	<body>
		<div class="container">
			<div class="sidebar">
				<h2>Log Files</h2>
				<ul id="logList" class="log-list"></ul>
			</div>
			<div class="log-viewer">
				<h2>Log Content</h2>
				<div id="logContent" class="log-content"></div>
			</div>
		</div>

		<script>
			let ws;
			let selectedLog = null;
			let reconnectAttempts = 0;
			const maxReconnectAttempts = 5;
			const reconnectDelay = 1000; // Start with 1 second delay

			async function loadLogs() {
				const response = await fetch('/logs');
				const logs = await response.json();
				const logList = document.getElementById('logList');
				logList.innerHTML = '';
				
				logs.forEach(log => {
					const li = document.createElement('li');
					li.className = 'log-item';
					li.textContent = log.name;
					li.onclick = () => selectLog(log.path);
					logList.appendChild(li);
				});
			}

			function connectWebSocket(path) {
				if (ws) {
					ws.close();
				}

				ws = new WebSocket('ws://' + window.location.host + '/ws?file=' + encodeURIComponent(path));
				
				ws.onopen = function() {
					console.log('Connected to WebSocket');
					reconnectAttempts = 0;
				};

				ws.onmessage = function(event) {
					try {
						const logContent = document.getElementById('logContent');
						const lines = event.data.split('\n');
						const fragment = document.createDocumentFragment();

						lines.forEach(line => {
							if (line) {
								const lineElement = document.createElement('div');
								if (path.endsWith('.leaks')) {
									lineElement.className = 'leak-line';
								}
								lineElement.textContent = line + '\n';
								fragment.appendChild(lineElement);
							}
						});

						logContent.appendChild(fragment);
						logContent.scrollTop = logContent.scrollHeight;
					} catch (error) {
						console.error('Error processing message:', error);
					}
				};

				ws.onclose = function(event) {
					console.log('WebSocket connection closed:', event.code, event.reason);
					if (!event.wasClean && reconnectAttempts < maxReconnectAttempts) {
						reconnectAttempts++;
						const delay = reconnectDelay * Math.pow(2, reconnectAttempts - 1);
						console.log('Reconnecting in ' + delay + 'ms...');
						setTimeout(() => connectWebSocket(path), delay);
					}
				};

				ws.onerror = function(error) {
					console.error('WebSocket error:', error);
					ws.close();
				};
			}

			function selectLog(path) {
				if (!path || selectedLog === path) return;

				selectedLog = path;
				document.querySelectorAll('.log-item').forEach(item => {
					item.classList.remove('selected');
					if (item.textContent === path) {
						item.classList.add('selected');
					}
				});

				const logContent = document.getElementById('logContent');
				logContent.innerHTML = '';

				connectWebSocket(path);
			}

			// Reload logs periodically to catch new files
			setInterval(loadLogs, 30000);

			// Initial load
			loadLogs();
		</script>
	</body>
	</html>
	`))
	tmpl.Execute(w, nil)
}

func handleGetLogs(w http.ResponseWriter, r *http.Request) {
	logFiles := []LogFile{}

	err := filepath.Walk("/tmp/log", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && (strings.HasSuffix(info.Name(), ".log") || strings.HasSuffix(info.Name(), ".leaks")) {
			logFiles = append(logFiles, LogFile{
				Name: info.Name(),
				Path: path,
			})
		}

		return nil
	})

	if err != nil {
		http.Error(w, "Failed to read log directory", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logFiles)
}