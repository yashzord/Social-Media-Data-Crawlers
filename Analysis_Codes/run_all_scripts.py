import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

scripts_dir = "/home/Data_Crawlers/project-2-implementation-elbaf/Analysis_Codes"

scripts = [
    "chan_general_trends.py",
    "common_plot_submissions_per_day.py",
    "common_plot_total_threads.py",
    "reddit_analysis_comment.py",
    "reddit_comment_upvote_distribution_politics.py",
    "reddit_graph_submission.py",
    "reddit_toxicity_graph.py",
    "reddit_upvote.py",
    "youtube_engagement_analysis.py",
    "youtube_highest_toxic_videos_analysis.py",
    "youtube_toxic_normal_comments.py"
]

def run_script(script_path):
    try:
        logging.info(f"Running script: {script_path}")
        result = subprocess.run(["python3", script_path], capture_output=True, text=True)
        if result.returncode == 0:
            logging.info(f"Script {script_path} executed successfully.")
        else:
            logging.error(f"Script {script_path} failed with return code {result.returncode}.")
            logging.error(f"Error output:\n{result.stderr}")
    except Exception as e:
        logging.error(f"An error occurred while running script {script_path}: {e}")

if __name__ == "__main__":
    for script in scripts:
        script_path = os.path.join(scripts_dir, script)
        if os.path.exists(script_path):
            run_script(script_path)
        else:
            logging.warning(f"Script not found: {script_path}")
