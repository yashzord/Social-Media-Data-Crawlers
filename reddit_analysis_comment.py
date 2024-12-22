import pymongo
import os
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
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
logger = logging.getLogger("HourlyCommentsAnalysis")

def save_plot(fig, filename):
    plot_path = os.path.join(PLOT_DIR, filename)
    fig.savefig(plot_path)
    plt.close(fig)
    logger.info(f"Plot saved to {plot_path}")

def get_comments_per_hour(start_date, end_date):
    """
    Retrieve hourly comment counts from the database between start_date and end_date.
    """
    hourly_counts = {date: {hour: 0 for hour in range(24)} for date in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1))}

    logger.info(f"Fetching posts from {start_date} to {end_date}...")
    posts = politics_collection.find({
        "crawled_at": {
            "$gte": start_date,
            "$lt": end_date + timedelta(days=1)  
        }
    })

    for post in posts:
        crawled_at = post['crawled_at']
        hour = crawled_at.hour
        date = crawled_at.date()

        if date not in hourly_counts:
            logger.warning(f"Date {date} not found in hourly_counts. Initializing it.")
            hourly_counts[date] = {hour: 0 for hour in range(24)}

        hourly_counts[date][hour] += post.get('comment_count', 0)

    logger.info("Finished retrieving hourly comment counts.")
    return hourly_counts

def plot_comments(hourly_counts):
    """
    Plot the hourly comment counts and save the plot.
    """
    overall_hours = []
    overall_values = []

    for date, counts in hourly_counts.items():
        for hour, count in counts.items():
            overall_hours.append(datetime.combine(date, datetime.min.time()) + timedelta(hours=hour))
            overall_values.append(count)

    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FFA07A", "#FF4500"])

    fig, ax = plt.subplots(figsize=(14, 7))
    colors = cmap(np.linspace(0, 1, len(overall_values)))

    for i in range(len(overall_hours) - 1):
        ax.plot(overall_hours[i:i + 2], overall_values[i:i + 2], color=colors[i], linewidth=2)

    scatter = ax.scatter(overall_hours, overall_values, c=overall_values, cmap=cmap, s=30)

    max_comments = max(overall_values)
    max_index = overall_values.index(max_comments)

    ax.plot(overall_hours[max_index], max_comments, 'ko', markersize=8, label='Highest Peak')

    ax.set_title('Total Number of Comments per Hour in a Day on r/politics (Nov 1 - Nov 14, 2024)', fontsize=14)
    ax.set_xlabel('Hour and Date', fontsize=12)
    ax.set_ylabel('Number of Comments', fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, linestyle='--', alpha=0.7)

    cbar = fig.colorbar(scatter, ax=ax)
    cbar.set_label('Number of Comments', fontsize=10)

    ax.legend()
    plt.tight_layout()

    save_plot(fig, "Comments_per_hour_nov_2024_gradient.png")

if __name__ == "__main__":
    start_date = datetime(2024, 11, 1)
    end_date = datetime(2024, 11, 14)

    hourly_counts = get_comments_per_hour(start_date, end_date)

    logger.info("Hourly Counts:")
    for date, counts in hourly_counts.items():
        logger.info(f"{date}: {counts}")

    plot_comments(hourly_counts)
