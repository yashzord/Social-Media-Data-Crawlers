import pymongo
import os
from dotenv import load_dotenv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['youtube_data']
videos_collection = db['videos']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Youtube_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)
plot_save_path = os.path.join(PLOT_DIR, "engagement_youtube.png")

channel_id_to_name = {
    "UCMiJRAwDNSNzuYeN2uWa0pA": "Tech Channel: Mrwhosetheboss",
    "UCGBzBkV-MinlBvHBzZawfLQ": "Movie Channel: Movie Central"
}

def fetch_video_metrics(channel_id):
    """
    Fetch video metrics (views, likes, comments) for a given channel.
    """
    logging.info(f"Fetching metrics for channel: {channel_id}")
    videos = list(videos_collection.find(
        {"channel_id": channel_id},
        {"_id": 0, "view_count": 1, "like_count": 1, "comment_count": 1}
    ))

    if not videos:
        logging.warning(f"No videos found for channel {channel_id}.")
        return 0, 0, 0

    total_views = sum(int(video.get('view_count', 0)) for video in videos)
    total_likes = sum(int(video.get('like_count', 0)) for video in videos)
    total_comments = sum(int(video.get('comment_count', 0)) for video in videos)

    logging.info(f"Channel {channel_id} - Views: {total_views}, Likes: {total_likes}, Comments: {total_comments}")
    return total_views, total_likes, total_comments

def plot_channel_metrics(data, save_path):
    """
    Plot the engagement metrics (views, likes, comments) for channels.
    """
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 8))
    ax = sns.barplot(x="Channel", y="Count", hue="Metric", data=data, palette="muted")

    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', fontsize=10, padding=3)

    plt.title("Views, Likes, and Comments Engagement", fontsize=14, fontweight="bold")
    plt.xlabel("Channel", fontsize=12, fontweight="bold")
    plt.ylabel("Count", fontsize=12, fontweight="bold")
    plt.tight_layout()

    logging.info(f"Saving plot to {save_path}...")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"Plot saved to {save_path}")

if __name__ == "__main__":
    channel_ids = os.getenv("YOUTUBE_CHANNELS", "").split(',')
    if not channel_ids:
        logging.error("No channel IDs found in the environment variable 'YOUTUBE_CHANNELS'.")
        exit(1)

    data = []
    for channel_id in channel_ids:
        total_views, total_likes, total_comments = fetch_video_metrics(channel_id)
        channel_name = channel_id_to_name.get(channel_id, channel_id)
        data.append({"Channel": channel_name, "Metric": "Views", "Count": total_views})
        data.append({"Channel": channel_name, "Metric": "Likes", "Count": total_likes})
        data.append({"Channel": channel_name, "Metric": "Comments", "Count": total_comments})

    df = pd.DataFrame(data)

    plot_channel_metrics(df, plot_save_path)
