import pymongo
import os
from dotenv import load_dotenv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

load_dotenv()

output_dir = r"/home/Data_Crawlers/project-2-implementation-elbaf/Final_Plots/Youtube_Plots"
os.makedirs(output_dir, exist_ok=True) 
save_path = os.path.join(output_dir, "highest_toxic_videos.png")

MONGO_DB_URL = os.getenv("MONGO_DB_URL")  
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['youtube_toxicity']
videos_collection = db['videos_toxicity']

channel_id_to_name = {
    "UCMiJRAwDNSNzuYeN2uWa0pA": "Tech Channel",
    "UCGBzBkV-MinlBvHBzZawfLQ": "Movie Channel"
}

def fetch_toxicity_per_video(channel_id):
    videos = list(videos_collection.find(
        {"channel_id": channel_id},
        {"_id": 0, "video_id": 1, "title": 1, "comments": 1}
    ))

    video_data = []

    for video in videos:
        video_id = video.get("video_id", "Unknown")
        title = video.get("title", "Unknown Title")
        comments = video.get("comments", [])
        toxic_count = 0

        for comment in comments:
            toxicity_data = comment.get('toxicity_data')
            if toxicity_data:
                is_toxic = toxicity_data.get('is_toxic')
                if is_toxic == "flag":
                    toxic_count += 1

        video_data.append({
            "video_id": video_id,
            "title": title,
            "toxic_count": toxic_count
        })

    return video_data

def plot_highest_toxic_videos(data, save_path):
    data['Video Label'] = data['Channel'] + ": " + data['Video Title']

    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(
        x="Toxic Comments",
        y="Video Label",
        data=data,
        palette="rocket",
        orient="h"
    )

    plt.title("Videos with the Highest Toxic Comments")
    plt.xlabel("Number of Toxic Comments")
    plt.ylabel("Video Title")
    plt.tight_layout()

    plt.savefig(save_path)
    plt.close()

channel_ids = os.getenv("YOUTUBE_CHANNELS").split(',')

all_video_data = []

for channel_id in channel_ids:
    channel_name = channel_id_to_name.get(channel_id, channel_id)  
    video_data = fetch_toxicity_per_video(channel_id)

    for video in video_data:
        all_video_data.append({
            "Channel": channel_name,
            "Video ID": video["video_id"],
            "Video Title": video["title"],
            "Toxic Comments": video["toxic_count"]
        })

df = pd.DataFrame(all_video_data)

top_toxic_videos = df.sort_values(by="Toxic Comments", ascending=False).head(10)

plot_highest_toxic_videos(top_toxic_videos, save_path)