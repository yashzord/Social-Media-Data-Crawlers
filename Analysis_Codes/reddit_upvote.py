import pymongo
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
import logging
from datetime import datetime, timedelta

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_data']
posts_collection = db['reddit_politics']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Reddit_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)
plot_save_path = os.path.join(PLOT_DIR, "reddit_upvotes_vs_comments.png")

def fetch_reddit_upvotes_comments(start_date, end_date):
    """
    Fetch upvotes and comments for posts within a given date range.
    """
    logging.info(f"Fetching Reddit posts from {start_date} to {end_date}...")
    posts = list(posts_collection.find({
        "submitted_at": {
            "$gte": start_date,
            "$lt": end_date + timedelta(days=1)
        }
    }, {"upvotes": 1, "comment_count": 1, "_id": 0}))

    upvotes = [post.get('upvotes', 0) for post in posts]
    comment_counts = [post.get('comment_count', 0) for post in posts]

    logging.info(f"Fetched {len(posts)} posts.")
    return upvotes, comment_counts

def plot_upvotes_vs_comments(upvotes, comment_counts, save_path):
    """
    Plot upvotes vs comment count as a scatter plot.
    """
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(upvotes, comment_counts, alpha=0.6, c=comment_counts, cmap='viridis', s=50)

    z = np.polyfit(upvotes, comment_counts, 1)
    p = np.poly1d(z)
    plt.plot(upvotes, p(upvotes), "r--", label=f"Trend Line: y={z[0]:.2f}x + {z[1]:.2f}")

    plt.colorbar(scatter, label="Comment Intensity")
    plt.title("Reddit Upvotes vs Comment Count", fontsize=14, fontweight="bold")
    plt.xlabel("Upvotes", fontsize=12, fontweight="bold")
    plt.ylabel("Comment Count", fontsize=12, fontweight="bold")
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    logging.info(f"Saving plot to {save_path}...")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"Plot saved to {save_path}")

if __name__ == "__main__":
    start_date = datetime(2024, 11, 1)
    end_date = datetime(2024, 11, 14)

    upvotes, comment_counts = fetch_reddit_upvotes_comments(start_date, end_date)
    if upvotes and comment_counts:
        plot_upvotes_vs_comments(upvotes, comment_counts, plot_save_path)
    else:
        logging.warning("No data available for the specified date range.")
