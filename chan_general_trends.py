import matplotlib.pyplot as plt
import pymongo
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import logging
import numpy as np
from collections import defaultdict

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)
db = client['4chan_data']
g_tv_threads_collection = db['g_tv_threads']
pol_threads_collection = db['pol_threads']

PLOT_DIR = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Chan_Plots"
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ChanAnalysis")

start_date = datetime(2024, 11, 1)
end_date = datetime(2024, 11, 14, 23, 59, 59)

def save_plot(fig, filename):
    plot_path = os.path.join(PLOT_DIR, filename)
    fig.savefig(plot_path)
    plt.close(fig)
    logger.info(f"Plot saved to {plot_path}")

def analyze_thread_activity_by_board(board):
    """
    Thread Activity Over Time
    """
    hourly_activity = defaultdict(lambda: {"threads_created": 0, "replies_received": 0})
    vibrant_colors = {"threads_created": "#FF6F61", "replies_received": "#6B5B95"}

    logger.info(f"Analyzing thread activity for /{board}/ board...")
    cursor = g_tv_threads_collection.find({
        "board": board,
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        op_created_at = datetime.strptime(thread['original_post']['OP_Created_at'], '%Y-%m-%d %H:%M:%S')
        hour_key = op_created_at.replace(minute=0, second=0)
        hourly_activity[hour_key]["threads_created"] += 1

        for reply in thread.get("replies", []):
            reply_created_at = datetime.strptime(reply['Reply_Created_at'], '%Y-%m-%d %H:%M:%S')
            hour_key = reply_created_at.replace(minute=0, second=0)
            hourly_activity[hour_key]["replies_received"] += 1

    hourly_keys = sorted(hourly_activity.keys())
    threads_created = [hourly_activity[key]["threads_created"] for key in hourly_keys]
    replies_received = [hourly_activity[key]["replies_received"] for key in hourly_keys]

    max_threads_created = max(threads_created)
    max_replies_received = max(replies_received)
    max_threads_index = threads_created.index(max_threads_created)
    max_replies_index = replies_received.index(max_replies_received)

    fig, ax = plt.subplots(figsize=(15, 8))
    ax.plot(hourly_keys, threads_created, label="Threads Created", color=vibrant_colors["threads_created"], marker="o")
    ax.plot(hourly_keys, replies_received, label="Replies Received", color=vibrant_colors["replies_received"], marker="x")

    ax.annotate(
        f"Peak: {max_threads_created}\nTime: {hourly_keys[max_threads_index]}",
        xy=(hourly_keys[max_threads_index], max_threads_created),
        xytext=(hourly_keys[max_threads_index], max_threads_created + 5),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10
    )
    ax.annotate(
        f"Peak: {max_replies_received}\nTime: {hourly_keys[max_replies_index]}",
        xy=(hourly_keys[max_replies_index], max_replies_received),
        xytext=(hourly_keys[max_replies_index], max_replies_received + 5),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10
    )

    ax.set_xlabel("Time (Hourly)", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.set_title(f"Thread Activity Over Time (/ {board} / Board)", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid()
    save_plot(fig, f"thread_activity_over_time_{board}.png")


def analyze_reply_frequency():
    """
    Reply Frequency: Average number of replies per thread categorized by board.
    """
    logger.info("Analyzing reply frequency...")
    reply_counts = defaultdict(list)

    cursor = g_tv_threads_collection.find({
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        board = thread.get("board", "unknown")
        reply_counts[board].append(len(thread.get("replies", [])))

    boards = sorted(reply_counts.keys())
    avg_replies = [sum(reply_counts[board]) / len(reply_counts[board]) for board in boards]

    vibrant_colors = ["#FF4500", "#32CD32"]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(boards, avg_replies, color=vibrant_colors)

    for bar, board, avg in zip(bars, boards, avg_replies):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"Board: {board}\nAvg: {avg:.2f}", ha="center", va="bottom", fontsize=10)

    ax.set_xlabel("Board", fontsize=12)
    ax.set_ylabel("Average Replies per Thread", fontsize=12)
    ax.set_title("Reply Frequency Per Thread Across Boards (/g/ and /tv/)", fontsize=14)
    ax.legend(["Average Replies"], fontsize=10)
    save_plot(fig, "reply_frequency_per_thread_by_board.png")


def analyze_thread_lifespan():
    """
    Thread Lifespan: Lifespan of the most popular thread by board.
    """
    logger.info("Analyzing thread lifespan...")
    most_popular_threads = {}

    cursor = g_tv_threads_collection.find({
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        board = thread.get("board", "unknown")
        replies = thread.get("replies", [])
        if replies:
            lifespan = (datetime.strptime(replies[-1]['Reply_Created_at'], '%Y-%m-%d %H:%M:%S') -
                        datetime.strptime(replies[0]['Reply_Created_at'], '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600
            most_popular_threads[board] = max(
                most_popular_threads.get(board, (0, None, 0)),
                (len(replies), thread['thread_number'], lifespan),
                key=lambda x: x[0]
            )

    boards = sorted(most_popular_threads.keys())
    lifespans = [most_popular_threads[board][2] for board in boards]
    thread_ids = [most_popular_threads[board][1] for board in boards]

    vibrant_colors = ["#FFB347", "#8A2BE2"]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(boards, lifespans, color=vibrant_colors)

    for bar, board, lifespan, thread_id in zip(bars, boards, lifespans, thread_ids):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"Board: {board}\nThread {thread_id}\n{lifespan:.2f} hrs",
                ha="center", va="bottom", fontsize=10)

    ax.set_xlabel("Board", fontsize=12)
    ax.set_ylabel("Lifespan (Hours)", fontsize=12)
    ax.set_title("Lifespan of Most Popular Threads (/g/ and /tv/)", fontsize=14)
    ax.legend(["Most Popular Thread"], fontsize=10)
    save_plot(fig, "most_popular_thread_lifespan_by_board.png")


def generate_popular_vs_unpopular_stacked_bar_chart():
    """
    Creating a stacked bar chart showing popular vs. unpopular threads for /g/ and /tv/ boards.
    Popular: 100 or more replies.
    Unpopular: Fewer than 100 replies.
    """
    logger.info("Generating stacked bar chart for popular vs. unpopular threads...")
    vibrant_colors = {"popular": "#4CAF50", "unpopular": "#FF6347"}

    board_data = defaultdict(lambda: {"popular": 0, "unpopular": 0})
    cursor = g_tv_threads_collection.find({
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        board = thread.get("board", "unknown")
        reply_count = len(thread.get("replies", []))
        if reply_count >= 100:
            board_data[board]["popular"] += 1
        else:
            board_data[board]["unpopular"] += 1

    boards = sorted(board_data.keys())
    popular_counts = [board_data[board]["popular"] for board in boards]
    unpopular_counts = [board_data[board]["unpopular"] for board in boards]

    fig, ax = plt.subplots(figsize=(10, 6))
    bar_width = 0.5
    bars_popular = ax.bar(boards, popular_counts, label="Popular Threads (≥100 replies)", color=vibrant_colors["popular"], width=bar_width)
    bars_unpopular = ax.bar(boards, unpopular_counts, bottom=popular_counts, label="Unpopular Threads (<100 replies)", color=vibrant_colors["unpopular"], width=bar_width)

    for i, (board, pop, unpop) in enumerate(zip(boards, popular_counts, unpopular_counts)):
        ax.text(i, pop, f"Board: {board}\n{pop} Popular", ha="center", va="bottom", fontsize=10)
        ax.text(i, pop + unpop, f"{unpop} Unpopular", ha="center", va="bottom", fontsize=10)

    ax.set_xlabel("Board", fontsize=12)
    ax.set_ylabel("Number of Threads", fontsize=12)
    ax.set_title("Popular vs. Unpopular Threads by Board (/g/ and /tv/)", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    save_plot(fig, "popular_vs_unpopular_threads_stacked_bar_chart.png")


def analyze_pol_posts():
    """
    Analyzing /pol/ posts for hourly activity and printing daily counts.
    """
    hourly_counts = defaultdict(lambda: {"original_posts": 0, "replies": 0, "total_posts": 0})
    daily_summary = defaultdict(lambda: {"original_posts": 0, "replies": 0, "total_posts": 0})

    logger.info("Analyzing /pol/ post activity...")
    cursor = pol_threads_collection.find({
        "board": "pol",
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        op_created_at = datetime.strptime(thread['original_post']['OP_Created_at'], '%Y-%m-%d %H:%M:%S')
        hour_key = op_created_at.replace(minute=0, second=0)
        day_key = op_created_at.date()
        hourly_counts[hour_key]["original_posts"] += 1
        hourly_counts[hour_key]["total_posts"] += 1
        daily_summary[day_key]["original_posts"] += 1
        daily_summary[day_key]["total_posts"] += 1

        for reply in thread.get("replies", []):
            reply_created_at = datetime.strptime(reply['Reply_Created_at'], '%Y-%m-%d %H:%M:%S')
            hour_key = reply_created_at.replace(minute=0, second=0)
            day_key = reply_created_at.date()
            hourly_counts[hour_key]["replies"] += 1
            hourly_counts[hour_key]["total_posts"] += 1
            daily_summary[day_key]["replies"] += 1
            daily_summary[day_key]["total_posts"] += 1

    for day, counts in sorted(daily_summary.items()):
        logger.info(f"{day}: {counts['original_posts']} original posts, {counts['replies']} replies, {counts['total_posts']} total posts")

    hourly_keys = sorted(hourly_counts.keys())
    total_posts_per_hour = [hourly_counts[key]["total_posts"] for key in hourly_keys]

    max_total_posts = max(total_posts_per_hour)
    max_hour_index = total_posts_per_hour.index(max_total_posts)
    peak_hour = hourly_keys[max_hour_index]

    daily_ticks = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    fig, ax = plt.subplots(figsize=(15, 8))
    ax.plot(hourly_keys, total_posts_per_hour, marker="o", linestyle="-", linewidth=1, label="Total Posts (Original + Replies)", color="#FF6347")

    ax.annotate(
        f"Peak: {max_total_posts}\nHour: {peak_hour.strftime('%Y-%m-%d %H:%M')}",
        xy=(peak_hour, max_total_posts),
        xytext=(peak_hour, max_total_posts + 100),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10
    )

    ax.set_xlabel("Date and Hour", fontsize=12)
    ax.set_ylabel("Total Posts (Original + Replies)", fontsize=12)
    ax.set_title("Number of Posts to /pol/ (Hourly) from Nov 1 to Nov 14, 2024", fontsize=14)
    ax.set_xticks(daily_ticks)
    ax.set_xticklabels([tick.strftime('%Y-%m-%d') for tick in daily_ticks], rotation=45, ha="right")
    ax.legend(fontsize=10)
    ax.grid()
    save_plot(fig, "pol_posts_binned_hourly.png")


def generate_hourly_activity_heatmap():
    """
    Create a heatmap showing hourly activity (original posts + replies) by day.
    """
    logger.info("Generating heatmap for hourly activity...")

    days = (end_date - start_date).days + 1
    hourly_activity = np.zeros((days, 24))

    cursor = g_tv_threads_collection.find({
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        op_created_at = datetime.strptime(thread['original_post']['OP_Created_at'], '%Y-%m-%d %H:%M:%S')
        day_index = (op_created_at.date() - start_date.date()).days
        if 0 <= day_index < days:  # Boundary check
            hourly_activity[day_index, op_created_at.hour] += 1

        for reply in thread.get("replies", []):
            reply_created_at = datetime.strptime(reply['Reply_Created_at'], '%Y-%m-%d %H:%M:%S')
            day_index = (reply_created_at.date() - start_date.date()).days
            if 0 <= day_index < days:  # Boundary check
                hourly_activity[day_index, reply_created_at.hour] += 1

    max_activity = np.max(hourly_activity)
    max_day, max_hour = np.unravel_index(np.argmax(hourly_activity), hourly_activity.shape)
    peak_date = start_date + timedelta(days=int(max_day))  # Convert numpy.int64 to int

    fig, ax = plt.subplots(figsize=(12, 8))
    cax = ax.imshow(hourly_activity, aspect="auto", cmap="YlGnBu")
    fig.colorbar(cax, ax=ax, label="Activity (Posts + Replies)")

    ax.set_xlabel("Hour of the Day", fontsize=12)
    ax.set_ylabel("Day", fontsize=12)
    ax.set_xticks(np.arange(24))
    ax.set_yticks(np.arange(days))
    ax.set_xticklabels(range(24))
    ax.set_yticklabels([(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)])
    ax.set_title("Hourly Activity Heatmap (/g/ and /tv/ Boards)", fontsize=14)

    ax.text(max_hour, max_day, f"{int(max_activity)}\n{peak_date.strftime('%Y-%m-%d')} {max_hour}:00", 
            ha="center", va="center", color="red", fontsize=10, bbox=dict(facecolor="white", edgecolor="red"))

    plt.tight_layout()
    save_plot(fig, "hourly_activity_heatmap.png")


def generate_thread_popularity_histogram(board):
    """
    Create a histogram showing the distribution of threads based on reply counts for a specified board.
    """
    logger.info(f"Generating histogram for thread popularity on /{board}/ board...")
    vibrant_color = "#FFB347"

    reply_counts = []
    cursor = g_tv_threads_collection.find({
        "board": board,
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        reply_counts.append(len(thread.get("replies", [])))

    bins = np.arange(0, max(reply_counts) + 10, 10)  # Create bins of size 10
    hist, bin_edges = np.histogram(reply_counts, bins=bins)

    peak_bin_index = np.argmax(hist)
    peak_bin_start = bin_edges[peak_bin_index]
    peak_bin_end = bin_edges[peak_bin_index + 1]
    peak_threads = hist[peak_bin_index]

    total_replies_up_to_peak = sum([count for count in reply_counts if peak_bin_start <= count < peak_bin_end])

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(bin_edges[:-1], hist, width=np.diff(bin_edges), align="edge", color=vibrant_color, edgecolor="black", alpha=0.8)
    ax.set_xlabel("Number of Replies (Grouped by Bins of 10)", fontsize=12)
    ax.set_ylabel("Number of Threads", fontsize=12)
    ax.set_title(f"Thread Popularity Distribution (/ {board} / Board)", fontsize=14)
    ax.legend([f"/{board.upper()}/ Threads"], fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    peak_annotation = f"Peak Threads: {peak_threads}\nReplies in Bin: {total_replies_up_to_peak}\nRange: {int(peak_bin_start)}-{int(peak_bin_end)}"
    ax.annotate(
        peak_annotation,
        xy=(peak_bin_start + 5, peak_threads),
        xytext=(peak_bin_start + 30, peak_threads + 5),
        arrowprops=dict(facecolor="black", arrowstyle="->"),
        fontsize=10
    )

    plt.tight_layout()
    save_plot(fig, f"thread_popularity_histogram_{board}.png")



def generate_replies_vs_posts_stacked_bar_chart():
    """
    Create a stacked bar chart showing original posts vs replies for each board.
    """
    logger.info("Generating stacked bar chart for replies vs. original posts...")
    vibrant_colors = {"original_posts": "#008B8B", "replies": "#FFA07A"}

    board_data = defaultdict(lambda: {"original_posts": 0, "replies": 0})

    cursor = g_tv_threads_collection.find({
        "original_post.OP_Created_at": {
            "$gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "$lte": end_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    for thread in cursor:
        board = thread.get("board", "unknown")
        board_data[board]["original_posts"] += 1
        board_data[board]["replies"] += len(thread.get("replies", []))

    boards = sorted(board_data.keys())
    original_posts = [board_data[board]["original_posts"] for board in boards]
    replies = [board_data[board]["replies"] for board in boards]

    max_posts = max(original_posts)
    max_replies = max(replies)

    fig, ax = plt.subplots(figsize=(10, 6))
    bar_width = 0.5
    ax.bar(boards, original_posts, label="Original Posts", color=vibrant_colors["original_posts"], width=bar_width)
    ax.bar(boards, replies, bottom=original_posts, label="Replies", color=vibrant_colors["replies"], width=bar_width)

    for i, (board, post, reply) in enumerate(zip(boards, original_posts, replies)):
        ax.text(i, post, f"{post}", ha="center", va="bottom", fontsize=10)
        ax.text(i, post + reply, f"{reply}", ha="center", va="bottom", fontsize=10)

    ax.set_xlabel("Board", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.set_title("Original Posts vs. Replies by Board (/g/ and /tv/)", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    save_plot(fig, "replies_vs_posts_stacked_bar_chart.png")

def analyze_pol_posts_daily():
    """
    Analyze /pol/ posts for daily activity.
    """
    daily_counts = defaultdict(lambda: {"original_posts": 0, "replies": 0, "total_posts": 0})

    logging.info("Analyzing /pol/ post activity (daily)...")
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
        print(f"{day}: Original Posts: {counts['original_posts']}, "
              f"Replies: {counts['replies']}, Total Posts: {counts['total_posts']}")

    days = sorted(daily_counts.keys())
    total_posts_per_day = [daily_counts[day]["total_posts"] for day in days]

    fig, ax = plt.subplots(figsize=(15, 8))
    ax.bar(days, total_posts_per_day, color="#FF6347", label="Total Posts (Original + Replies)")

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
    ax.set_ylabel("Total Posts (Original + Replies)", fontsize=12)
    ax.set_title("Daily Post Activity on /pol/ from Nov 1 to Nov 14, 2024", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=45, ha="right")
    save_plot(fig, "pol_posts_binned_daily.png")


if __name__ == "__main__":
    analyze_thread_activity_by_board("g")
    analyze_thread_activity_by_board("tv")
    analyze_reply_frequency()
    analyze_thread_lifespan()
    generate_popular_vs_unpopular_stacked_bar_chart()
    analyze_pol_posts()
    generate_hourly_activity_heatmap()
    generate_thread_popularity_histogram("g")
    generate_thread_popularity_histogram("tv")
    generate_replies_vs_posts_stacked_bar_chart()
    analyze_pol_posts_daily()

