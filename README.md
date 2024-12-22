Implementation: Data Crawlers

## Setup Instructions

### 1. Check System Settings
```bash
cat /etc/os-release
```

### 2. Navigate to the Project Directory
```bash
cd /home/Data_Crawlers/project-2-implementation-elbaf
```

### 3. Install Docker on the VM
```bash
sudo apt update
sudo apt install docker.io -y
sudo systemctl enable docker --now
docker --version
# Optional: Add user to docker group
# sudo usermod -aG docker ubuntu (or)
# sudo usermod -aG docker $USER
sudo service docker restart
```

### 4. Install MongoDB Using Docker
```bash
sudo docker pull mongo
sudo docker run --name mongodb -d -p 27017:27017 mongo
sudo docker start mongodb
sudo docker stop mongodb
```

### 5. Install MongoDB Compass on Local Machine
Follow the [MongoDB Compass installation guide](https://www.mongodb.com/docs/compass/current/install/) for your system.

### 6. Install Faktory Using Docker
```bash
sudo apt update
docker pull contribsys/faktory
```

### 7. Create and Activate a Virtual Environment
```bash
# Navigate to Data_Crawlers directory
cd Data_Crawlers
python -m venv ./env/dev
source env/dev/bin/activate
# To deactivate:
deactivate
```

### 8. Install Required Python Packages
After activating the virtual environment:
```bash
pip install -r requirements.txt
```

### 9. Start MongoDB (if not running already)
```bash
sudo docker start mongodb
```

### 10. Check Docker Containers
```bash
sudo docker ps -a
```

### 11. Stop Specific Containers (if needed)
```bash
sudo docker stop {container-name}
```

### 12. Start Faktory Server
Navigate to the Faktory server screen window and run:
```bash
sudo docker run --rm -it -v $(pwd)/data:/var/lib/faktory/db \
    -e "FAKTORY_PASSWORD=elbaf123" \
    -p 0.0.0.0:7419:7419 -p 0.0.0.0:7420:7420 contribsys/faktory:latest
```

### 13. Access Faktory UI
- Use a browser: `http://localhost:7420/`
- Or, from the local machine:
```bash
ssh -L 7420:localhost:7420 -p 22 USER@128.226.29.113
```

---

## Running the Crawlers

To manage the crawling scripts, we use `screen` to handle multiple terminal windows:

### 1. Start Screen and List Existing Sessions
```bash
screen -ls
```
If a session exists, attach to it:
```bash
screen -r {session-name}
```

### 2. Navigate to the Project Directory
```bash
cd /home/Data_Crawlers/project-2-implementation-elbaf
```

### 3. Use Existing Screen Windows for Each Process
Navigate to the appropriate screen window and run the corresponding command:

- **Window 1:** Start Faktory server
  ```bash
  sudo docker run --rm -it -v $(pwd)/data:/var/lib/faktory/db \
      -e "FAKTORY_PASSWORD=elbaf123" \
      -p 0.0.0.0:7419:7419 -p 0.0.0.0:7420:7420 contribsys/faktory:latest
  ```

- **Window 2:** Run `chan_moderate_crawler.py`
  ```bash
  python3 chan_moderate_crawler.py
  ```

- **Window 3:** Run `chan_toxicity_analysis.py`
  ```bash
  python3 chan_toxicity_analysis.py
  ```

- **Window 4:** Run `chan_crawler.py`
  ```bash
  python3 chan_crawler.py
  ```

- **Window 5:** Run `youtube_crawler.py`
  ```bash
  python3 youtube_crawler.py
  ```

- **Window 6:** Run `reddit_crawler.py`
  ```bash
  python3 reddit_crawler.py
  ```

- **Window 7:** Run `chan_old_threads_toxicity_analysis.py`
  ```bash
  python3 chan_old_threads_toxicity_analysis.py
  ```

This ensures all crawlers are running and can be controlled individually.

---

## Running Analysis Scripts

1. Navigate to the Analysis Scripts Directory:
```bash
cd /home/Data_Crawlers/project-2-implementation-elbaf/Analysis_Codes
```

2. Run All Analysis Scripts:
```bash
python3 run_all_scripts.py
```

This will generate analysis plots and graphs, which are stored in the `Final_Plots` directory:

- **Chan_Plots**: 4chan-related plots
- **Reddit_Plots**: Reddit-related plots
- **Youtube_Plots**: YouTube-related plots
- **Common_Plots**: Combined datasets from Reddit, 4chan, and YouTube

Currently, there are 25 analysis plots available.

---

### Additional Notes
- To detach from a screen session: `Ctrl+A D`
- To list all windows in a session: `screen -X windows`
- Access plots in `/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/`.
