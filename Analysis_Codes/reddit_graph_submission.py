import pymongo
import os
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.cm import ScalarMappable
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL", "mongodb://128.226.29.113:27017/reddit_data")
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_data']
politics_collection = db['reddit_politics']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Reddit_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RedditDailyPosts")

def save_plot(fig, filename):
    plot_path = os.path.join(PLOT_DIR, filename)
    fig.savefig(plot_path)
    plt.close(fig)
    logger.info(f"Plot saved to {plot_path}")

def get_submissions_per_day(start_date, end_date):
    """
    Fetch Reddit submissions within the specified date range and count them per day.
    """
    daily_counts = {}

    logger.info(f"Fetching submissions from {start_date} to {end_date}...")
    posts = politics_collection.find({
        "submitted_at": {
            "$gte": start_date,
            "$lt": end_date + timedelta(days=1)
        }
    })

    for post in posts:
        submitted_at = post.get('submitted_at')
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
        
        date = submitted_at.date()
        daily_counts[date] = daily_counts.get(date, 0) + 1

    logger.info("Finished fetching daily submission counts.")
    return daily_counts

def plot_submissions_enhanced(daily_counts):
    """
    Generate a visually enhanced bar plot for daily submission counts.
    """
    dates = list(daily_counts.keys())
    counts = list(daily_counts.values())

    colors = np.linspace(0, 1, len(dates))
    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FFA07A", "#FF4500"])  
    bar_colors = cmap(colors)

    fig, ax = plt.subplots(figsize=(16, 8))

    bars = ax.bar(dates, counts, color=bar_colors, edgecolor='darkgray', linewidth=0.7)

    ax.set_title('Number of Reddit Politics Posts Submitted Per Day', fontsize=18, fontweight='bold', pad=20)
    ax.set_xlabel('Date', fontsize=14, labelpad=10)
    ax.set_ylabel('Number of Posts', fontsize=14, labelpad=10)

    ax.set_xticks(dates)
    ax.set_xticklabels([date.strftime('%b %d') for date in dates], rotation=45, fontsize=12)
    ax.tick_params(axis='y', labelsize=12)

    for bar, count in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1, str(count), ha='center', va='bottom', fontsize=10, color='darkblue')

    sm = ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=min(counts), vmax=max(counts)))
    sm.set_array([])  
    cbar = fig.colorbar(sm, ax=ax, aspect=30, pad=0.02)
    cbar.set_label('Post Count Intensity', fontsize=12)
    cbar.ax.tick_params(labelsize=10)

    ax.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()

    save_plot(fig, "Reddit_posts_per_day.png")

if __name__ == "__main__":
    start_date = datetime(2024, 11, 1)
    end_date = datetime(2024, 11, 14)

    daily_counts = get_submissions_per_day(start_date, end_date)

    logger.info("Daily Counts:")
    for date, count in daily_counts.items():
        logger.info(f"{date}: {count}")

    plot_submissions_enhanced(daily_counts)

    total_posts = sum(daily_counts.values())
    logger.info(f"\nTotal posts crawled: {total_posts}")
