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
db = mongo_client['youtube_toxicity']
videos_collection = db['videos_toxicity']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Youtube_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)
plot_save_path = os.path.join(PLOT_DIR, "toxic_vs_normal_comments.png")

channel_id_to_name = {
    "UCMiJRAwDNSNzuYeN2uWa0pA": "Tech Channel: Mrwhosetheboss",
    "UCGBzBkV-MinlBvHBzZawfLQ": "Movie Channel: Movie Central"
}

def fetch_toxicity_data(channel_id):
    """
    Fetch toxic and normal comment counts for a given channel.
    """
    logging.info(f"Fetching toxicity data for channel: {channel_id}")
    videos = list(videos_collection.find(
        {"channel_id": channel_id},
        {"_id": 0, "comments": 1, "title": 1}
    ))

    if not videos:
        logging.warning(f"No comments found for channel {channel_id}.")
        return 0, 0

    toxic_count = 0
    normal_count = 0
    for video in videos:
        comments = video.get('comments', [])
        for comment in comments:
            toxicity_data = comment.get('toxicity_data')
            if toxicity_data:
                is_toxic = toxicity_data.get('is_toxic')
                if is_toxic == "flag":
                    toxic_count += 1
                else:
                    normal_count += 1
            else:
                normal_count += 1

    logging.info(f"Channel {channel_id} - Toxic Comments: {toxic_count}, Normal Comments: {normal_count}")
    return toxic_count, normal_count

def fetch_video_count(channel_id):
    """
    Fetch the total number of videos for a given channel.
    """
    video_count = videos_collection.count_documents({"channel_id": channel_id})
    logging.info(f"Channel {channel_id} - Total Videos: {video_count}")
    return video_count

def plot_combined_toxic_vs_normal(data, save_path):
    """
    Plot the comparison of toxic vs normal comments.
    """
    sns.set(style="whitegrid")
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(x="Channel", y="Count", hue="Category", data=data, palette="pastel")

    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', fontsize=10, padding=3)

    plt.title("Toxic vs Normal Comments Comparison", fontsize=14, fontweight="bold")
    plt.xlabel("Channel", fontsize=12, fontweight="bold")
    plt.ylabel("Number of Comments", fontsize=12, fontweight="bold")
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
        toxic_count, normal_count = fetch_toxicity_data(channel_id)
        video_count = fetch_video_count(channel_id)
        channel_name = channel_id_to_name.get(channel_id, channel_id)
        data.append({"Channel": channel_name, "Category": "Toxic Comments", "Count": toxic_count})
        data.append({"Channel": channel_name, "Category": "Normal Comments", "Count": normal_count})

    df = pd.DataFrame(data)

    plot_combined_toxic_vs_normal(df, plot_save_path)
