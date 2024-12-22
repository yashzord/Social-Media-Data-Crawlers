import pymongo
import matplotlib.pyplot as plt
import numpy as np
import logging
from datetime import datetime
from matplotlib.colors import LinearSegmentedColormap
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_DB_URL = "mongodb://128.226.29.113:27017/reddit_data"
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_Data_moderate_speech']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Reddit_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

collections = {
    "technology": db['posts'],
    "movies": db['posts'],
    "politics": db['reddit_politics']
}

start_date = datetime(2024, 11, 21)
end_date = datetime(2024, 11, 28)

moderation_counts_comments = {}

for subreddit, collection in collections.items():
    logging.info(f"Processing subreddit: {subreddit}")
    comment_pipeline = [
        {"$match": {"subreddit": subreddit}},
        {"$unwind": "$comments"},
        {"$unwind": "$crawl_history"},
        {"$match": {
            "crawl_history": {"$gte": start_date, "$lte": end_date}
        }},
        {"$group": {
            "_id": "$comments.moderate_class",
            "count": {"$sum": 1}
        }}
    ]
    comment_result = list(collection.aggregate(comment_pipeline))
    moderation_counts_comments[subreddit] = {entry["_id"]: entry["count"] for entry in comment_result}

logging.info("\n--- Moderation Counts for Comments ---")
for subreddit, counts in moderation_counts_comments.items():
    logging.info(f"{subreddit} moderation counts: {counts}")

def save_plot(fig, filename):
    plot_path = os.path.join(PLOT_DIR, filename)
    fig.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    logging.info(f"Chart saved as {plot_path}")

def create_comment_bar_chart(data, title, ylabel, filename):
    categories = ["normal", "flag", None]
    subreddits = list(data.keys())
    
    bar_width = 0.25
    index = np.arange(len(subreddits))
    
    values_normal = [data[subreddit].get("normal", 0) for subreddit in subreddits]
    values_flagged = [data[subreddit].get("flag", 0) for subreddit in subreddits]
    values_none = [data[subreddit].get(None, 0) for subreddit in subreddits]

    cmap_normal = LinearSegmentedColormap.from_list("gradient_normal", ["#87CEEB", "#4682B4"])
    cmap_flagged = LinearSegmentedColormap.from_list("gradient_flagged", ["#FFD700", "#FF4500"])
    cmap_none = LinearSegmentedColormap.from_list("gradient_none", ["#D3D3D3", "#696969"])

    gradient_normal = cmap_normal(np.linspace(0, 1, len(subreddits)))
    gradient_flagged = cmap_flagged(np.linspace(0, 1, len(subreddits)))
    gradient_none = cmap_none(np.linspace(0, 1, len(subreddits)))

    fig, ax = plt.subplots(figsize=(14, 8))
    
    for i in range(len(subreddits)):
        normal_bar = ax.bar(index[i], values_normal[i], bar_width, color=gradient_normal[i], label='Normal' if i == 0 else "", alpha=0.8)
        flagged_bar = ax.bar(index[i] + bar_width, values_flagged[i], bar_width, color=gradient_flagged[i], label='Flagged' if i == 0 else "", alpha=0.8)
        none_bar = ax.bar(index[i] + 2 * bar_width, values_none[i], bar_width, color=gradient_none[i], label='Unclassified' if i == 0 else "", alpha=0.8)

        ax.text(normal_bar[0].get_x() + normal_bar[0].get_width()/2., normal_bar[0].get_height(),
                f'{values_normal[i]:,}', ha='center', va='bottom', rotation=90, fontsize=8, fontweight='bold')
        ax.text(flagged_bar[0].get_x() + flagged_bar[0].get_width()/2., flagged_bar[0].get_height(),
                f'{values_flagged[i]:,}', ha='center', va='bottom', rotation=90, fontsize=8, fontweight='bold')
        ax.text(none_bar[0].get_x() + none_bar[0].get_width()/2., none_bar[0].get_height(),
                f'{values_none[i]:,}', ha='center', va='bottom', rotation=90, fontsize=8, fontweight='bold')

    ax.set_yscale('log')
    ax.set_xlabel("Subreddits", fontsize=12, fontweight="bold")
    ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xticks(index + bar_width)
    ax.set_xticklabels(subreddits, fontsize=10)
    ax.legend(title="Moderation Class", fontsize=10)
    
    ax.grid(True, which="both", ls="-", alpha=0.2)
    
    plt.tight_layout()
    save_plot(fig, filename)

create_comment_bar_chart(
    moderation_counts_comments,
    "Moderation Counts for Comments",
    "Number of Comments (Log Scale)",
    "moderation_counts_comments.png"
)

logging.info("Script execution completed.")
