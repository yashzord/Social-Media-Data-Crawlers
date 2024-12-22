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

youtube_db = mongo_client['youtube_data']
channels_collection = youtube_db['channels']

chan_db = mongo_client['4chan_data']
chan_threads_collection = chan_db['g_tv_threads']

reddit_db = mongo_client['reddit_data']
reddit_posts_collection = reddit_db['posts']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Common_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)
plot_save_path = os.path.join(PLOT_DIR, "three_platforms_engagement.png")

def fetch_total_counts():
    """
    Fetch total counts from YouTube, 4chan, and Reddit.
    """
    logging.info("Fetching total video counts from YouTube...")
    youtube_video_count = channels_collection.aggregate([
        {"$group": {"_id": None, "total_videos": {"$sum": "$video_count"}}}
    ])
    total_videos = list(youtube_video_count)[0]["total_videos"] if youtube_video_count else 0

    logging.info("Fetching total thread counts from 4chan...")
    total_threads = chan_threads_collection.count_documents({})

    logging.info("Fetching total post counts from Reddit...")
    total_posts = reddit_posts_collection.count_documents({})

    logging.info(f"Total videos: {total_videos}, Total threads: {total_threads}, Total posts: {total_posts}")
    return total_videos, total_threads, total_posts

def plot_total_counts(data, save_path):
    """
    Plot total counts and save the plot.
    """
    sns.set(style="whitegrid")
    plt.figure(figsize=(10, 6))

    logging.info("Creating bar plot for total counts...")
    ax = sns.barplot(x="Category", y="Count", data=data, palette="muted")

    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', fontsize=10, padding=3)

    colors = sns.color_palette("muted", n_colors=len(data))
    legend_labels = data["Category"].unique()
    legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in colors]
    plt.legend(legend_handles, legend_labels, loc="upper right")

    plt.title("Videos, Threads, and Posts Engagement", fontsize=14, fontweight="bold")
    plt.xlabel("Category", fontsize=12, fontweight="bold")
    plt.ylabel("Count", fontsize=12, fontweight="bold")
    plt.tight_layout()

    logging.info(f"Saving plot to {save_path}...")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"Plot saved to {save_path}")

if __name__ == "__main__":
    total_videos, total_threads, total_posts = fetch_total_counts()
    data = pd.DataFrame([
        {"Category": "Videos", "Count": total_videos},
        {"Category": "Threads", "Count": total_threads},
        {"Category": "Posts", "Count": total_posts}
    ])
    plot_total_counts(data, plot_save_path)
