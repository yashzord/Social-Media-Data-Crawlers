import pymongo
import os
import matplotlib.pyplot as plt
import numpy as np
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_DB_URL = "mongodb://128.226.29.113:27017/reddit_data"
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_Data_moderate_speech']

collections = {
    "technology": db['posts'],
    "movies": db['posts'],
    "politics": db['reddit_politics']
}

start_date = datetime(2024, 11, 21)
end_date = datetime(2024, 11, 28)

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Reddit_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

def get_comment_upvotes(collection, subreddit):
    """
    Fetch comment upvotes from a collection for a given subreddit within the specified date range.
    """
    logging.info(f"Fetching comment upvotes for subreddit: {subreddit}")
    pipeline = [
        {"$match": {"subreddit": subreddit, "crawl_history": {"$gte": start_date, "$lte": end_date}}},
        {"$unwind": "$comments"},
        {"$group": {
            "_id": "$post_id",
            "comment_upvotes": {"$push": "$comments.upvote_score"},
            "post_title": {"$first": "$post_title"}
        }}
    ]
    return list(collection.aggregate(pipeline))

def plot_comment_upvotes(data, subreddit, filename):
    """
    Plot the upvote distribution for comments in a subreddit.
    """
    fig, ax = plt.subplots(figsize=(15, 8))

    for i, post in enumerate(data):
        y = sorted(post['comment_upvotes'], reverse=True)
        x = range(len(y))
        ax.plot(x, y, marker='o', linestyle='-', markersize=4, alpha=0.7, label=f"Post {i+1}")
    
    ax.set_xlabel("Comment Rank", fontsize=12, fontweight="bold")
    ax.set_ylabel("Upvote Score", fontsize=12, fontweight="bold")
    ax.set_title(f"Comment Upvote Distribution for {subreddit}", fontsize=14, fontweight="bold")
    ax.set_yscale('symlog') 
    ax.grid(True, which="both", ls="-", alpha=0.2)
    
    plt.tight_layout()
    plot_path = os.path.join(PLOT_DIR, filename)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"Chart saved as {plot_path}")

if __name__ == "__main__":
    for subreddit, collection in collections.items():
        logging.info(f"Processing subreddit: {subreddit}")
        comment_data = get_comment_upvotes(collection, subreddit)
        if comment_data:
            plot_comment_upvotes(comment_data, subreddit, f"{subreddit}_comment_upvotes.png")
        else:
            logging.warning(f"No data available for subreddit: {subreddit}")

    logging.info("Script execution completed.")
