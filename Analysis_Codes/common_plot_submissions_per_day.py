import matplotlib.pyplot as plt
import pymongo
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import logging
from collections import defaultdict

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)

chan_db = client['4chan_data']
reddit_db = client['reddit_data']
pol_threads_collection = chan_db['pol_threads']
politics_collection = reddit_db['reddit_politics']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Common_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CombinedAnalysis")

start_date = datetime(2024, 11, 1)
end_date = datetime(2024, 11, 14, 23, 59, 59)

def save_plot(fig, filename):
    plot_path = os.path.join(PLOT_DIR, filename)
    fig.savefig(plot_path)
    plt.close(fig)
    logger.info(f"Plot saved to {plot_path}")

def analyze_pol_posts_daily():
    """
    Analyze 4chan /pol/ posts for daily activity.
    """
    daily_counts = defaultdict(lambda: {"original_posts": 0, "replies": 0, "total_posts": 0})

    logger.info("Analyzing 4chan /pol/ post activity (daily)...")
    cursor = pol_threads_collection.find({
        "board": "pol",
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        op_created_at = datetime.strptime(thread['original_post']['OP_Created_at'], '%Y-%m-%d %H:%M:%S')
        day_key = op_created_at.date()
        daily_counts[day_key]["original_posts"] += 1
        daily_counts[day_key]["total_posts"] += 1

        for reply in thread.get("replies", []):
            reply_created_at = datetime.strptime(reply['Reply_Created_at'], '%Y-%m-%d %H:%M:%S')
            day_key = reply_created_at.date()
            daily_counts[day_key]["replies"] += 1
            daily_counts[day_key]["total_posts"] += 1

    for day, counts in sorted(daily_counts.items()):
        print(f"4chan /pol/ - {day}: Original Posts: {counts['original_posts']}, "
              f"Replies: {counts['replies']}, Total Posts: {counts['total_posts']}")

    days = sorted(daily_counts.keys())
    total_posts_per_day = [daily_counts[day]["total_posts"] for day in days]

    fig, ax = plt.subplots(figsize=(15, 8))
    ax.plot(days, total_posts_per_day, marker="o", color="#FF6347", label="Total Posts (/pol/)")

    max_total_posts = max(total_posts_per_day)
    peak_day_index = total_posts_per_day.index(max_total_posts)
    peak_day = days[peak_day_index]

    ax.annotate(
        f"Peak: {max_total_posts}\nDate: {peak_day.strftime('%Y-%m-%d')}",
        xy=(peak_day, max_total_posts),
        xytext=(peak_day, max_total_posts + 100),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10
    )

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Total Posts", fontsize=12)
    ax.set_title("Daily Post Activity on 4chan /pol/ (Nov 1 to Nov 14, 2024)", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=45, ha="right")
    save_plot(fig, "pol_posts_binned_daily.png")

    return {day: daily_counts[day]["total_posts"] for day in daily_counts}


def analyze_politics_submissions_daily():
    """
    Analyze Reddit r/politics submissions for daily activity.
    """
    daily_counts = defaultdict(int)

    logger.info("Analyzing Reddit r/politics submissions (daily)...")
    cursor = politics_collection.find({
        "submitted_at": {
            "$gte": start_date,
            "$lte": end_date
        }
    })

    for submission in cursor:
        submitted_at = submission.get("submitted_at")
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
        day_key = submitted_at.date()
        daily_counts[day_key] += 1

    for day, count in sorted(daily_counts.items()):
        print(f"Reddit r/politics - {day}: Total Submissions: {count}")

    days = sorted(daily_counts.keys())
    submissions_per_day = [daily_counts[day] for day in days]

    fig, ax = plt.subplots(figsize=(15, 8))
    ax.plot(days, submissions_per_day, marker="o", color="#4682B4", label="Total Submissions (r/politics)")

    max_submissions = max(submissions_per_day)
    peak_day_index = submissions_per_day.index(max_submissions)
    peak_day = days[peak_day_index]

    ax.annotate(
        f"Peak: {max_submissions}\nDate: {peak_day.strftime('%Y-%m-%d')}",
        xy=(peak_day, max_submissions),
        xytext=(peak_day, max_submissions + 10),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10
    )

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Total Submissions", fontsize=12)
    ax.set_title("Daily Submission Activity on Reddit r/politics (Nov 1 to Nov 14, 2024)", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=45, ha="right")
    save_plot(fig, "politics_submissions_daily.png")

    return daily_counts


def plot_combined_activity():
    """
    Plot combined daily activity for 4chan /pol/ and Reddit r/politics.
    """
    pol_counts = analyze_pol_posts_daily()
    politics_counts = analyze_politics_submissions_daily()

    days = sorted(set(pol_counts.keys()).union(set(politics_counts.keys())))
    pol_activity = [pol_counts.get(day, 0) for day in days]
    politics_activity = [politics_counts.get(day, 0) for day in days]

    max_pol = max(pol_activity)
    peak_pol_day = days[pol_activity.index(max_pol)]

    max_politics = max(politics_activity)
    peak_politics_day = days[politics_activity.index(max_politics)]

    fig, ax = plt.subplots(figsize=(15, 8))
    ax.plot(days, pol_activity, marker="o", color="#FF6347", label="4chan /pol/")
    ax.plot(days, politics_activity, marker="x", color="#4682B4", label="Reddit r/politics")

    ax.annotate(
        f"Peak: {max_pol}\nDate: {peak_pol_day.strftime('%Y-%m-%d')}",
        xy=(peak_pol_day, max_pol),
        xytext=(peak_pol_day, max_pol - max_pol * 0.1),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10,
        color="#FF6347"
    )
    ax.annotate(
        f"Peak: {max_politics}\nDate: {peak_politics_day.strftime('%Y-%m-%d')}",
        xy=(peak_politics_day, max_politics),
        xytext=(peak_politics_day, max_politics - max_politics * 0.1),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10,
        color="#4682B4"
    )

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Total Activity", fontsize=12)
    ax.set_title("Combined Daily Activity: 4chan /pol/ vs Reddit r/politics (Nov 1 to Nov 14, 2024)", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=45, ha="right")

    save_plot(fig, "combined_activity_pol_politics.png")


if __name__ == "__main__":
    plot_combined_activity()
