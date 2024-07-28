from datetime import datetime
from dotenv import load_dotenv
from flask import request, jsonify, Flask
from io import BytesIO
from save import save_videos, supabase, index
from tasks import process_videos, query
from werkzeug.utils import secure_filename
from zipfile import ZipFile
import json


load_dotenv()
# Supabase setup
# url = os.environ.get("SUPABASE_URL")
# key = os.environ.get("SUPABASE_KEY")
# supabase = create_client(url, key)
flask_app = Flask(__name__)
CHECK_TYPES = {1: process_videos, 2: query}
# PROCEDURE
# INPUT: array of tiktok links/tiktok ids, user_id

# query supabase, if vector already exists in another name space, simply move that tiktok with
# its associated namespace
# to another list, do so for all tiktoks, then pass the ones not associated with any namespace to
# process_videos(), then pass the ones already exist to upload_vectors()

# OUTPUT: two ids, associated with process_video and upload_vector background workers

# FUNCTIONS

# INPUT: Vector[] or tuple of (tiktok id,namespace), new_user_namespace
# upload_vector(Array of tuple(tiktok id, Vector) or tuple(tiktok id,namespace), new_user_namespace)
# Either upload the vector as a batch under new_user_namespace
# Or query and associate the vector matching to tiktok id and namespace to new_user_namespace
# Output: Success or failure with message from API call to pinecone
# Problems: what if successful upload to PineCone but unsuccessful data update in the favorite video
# table of supabase

# INPUT: dictionary of Tiktok video information
# vectorize_video() should create string based on video info, convert to vector and return that vector
# OUTPUT: vector that represents the video

# INPUT: Array of tiktok ids
# process_videos()
# get tiktok video info for each id, if failure add to failed array of ids, if success add info
# to success array; success array composed of tuples (tiktok id, video info dict)
# Once len(success) == 50 or less (if num videos <50), map vectorize_video(video info dict) of the tuples on
# success array then call upload_vector(success, user_id) then
# call supabase_update_user_favorite_video(success, user_id)
# continue until no more videos
# OUTPUT: failed tiktok ids


ALLOWED_EXTENSIONS = {"zip"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def video_url_to_id(url):
    """Converts a TikTok URL to the relevant ID."""

    return url.split("/")[-2]


@flask_app.route("/api/v1/verify", methods=["POST"])
async def verify_login():
    jwt = request.headers.get("Authorization")
    try:
        user_id = supabase.auth.get_user(jwt).user.id
    except Exception as e:
        return json.dumps(str(e))
    # This block of code is handling the processing of uploaded zip files
    for uploaded_file in request.files.getlist("file"):
        if uploaded_file.filename != "" and allowed_file(uploaded_file.filename):
            file_stream = uploaded_file.read()
            myzip = ZipFile(BytesIO(file_stream))
            file_name = secure_filename(myzip.namelist()[0])
            print(f"reading {file_name}")
            # Reading the contents of a file inside a zip
            # archive and converting the bytes into a string.
            user_videos = ""
            for character in myzip.open(file_name).read():
                user_videos += chr(character)
            # sorts videos by most recent Date first, not sure if Date means date of adding to favorite or date of video release
            try:
                user_videos = sorted(
                    json.loads(user_videos)["Activity"]["Favorite Videos"][
                        "FavoriteVideoList"
                    ],
                    key=lambda e: datetime.strptime(e["Date"], "%Y-%m-%d %H:%M:%S"),
                    reverse=True,
                )
                user_videos = user_videos[:3]
                # filter out videos already in vector database before passing to save_videos
                # database call is required here..., maybe a blacklist of videos that is in failure.json
                res = await save_videos(user_videos, user_id)
                # limit to 3 videos for testing
            except Exception as e:
                print(e)
                res = None
                user_videos = []
    # Do something with user_videos
    return json.dumps(res)


@flask_app.post("/api/v1/submit")
def submit():
    jwt = request.headers.get("Authorization")
    print(jwt)
    try:
        user_id = supabase.auth.get_user(jwt).user.id
    except Exception as e:
        print(e)
        return json.dumps(str(e))
    # This block of code is handling the processing of uploaded zip files
    for uploaded_file in request.files.getlist("file"):
        if uploaded_file.filename != "" and allowed_file(uploaded_file.filename):
            file_stream = uploaded_file.read()
            myzip = ZipFile(BytesIO(file_stream))
            file_name = secure_filename(myzip.namelist()[0])
            print(f"reading {file_name}")
            # Reading the contents of a file inside a zip
            # archive and converting the bytes into a string.
            user_videos = ""
            for character in myzip.open(file_name).read():
                user_videos += chr(character)
            # sorts videos by most recent Date first, not sure if Date means date of adding to favorite or date of video release
            try:
                user_videos = sorted(
                    json.loads(user_videos)["Activity"]["Favorite Videos"][
                        "FavoriteVideoList"
                    ],
                    key=lambda e: datetime.strptime(e["Date"], "%Y-%m-%d %H:%M:%S"),
                    reverse=True,
                )
                # user_videos = user_videos[:3]
                # filter out videos already in vector database before passing to save_videos
                # database call is required here..., maybe a blacklist of videos that is in failure.json
                videos = (
                    supabase.table("user_videos")
                    .select("*")
                    .eq("id", user_id)
                    .execute()
                )
                if videos and videos.data:
                    videos = videos.data[0]["videos"]
                    limit = int(videos.data[0]["limit"])
                    if len(videos) > limit:
                        return json.dumps(
                            {"error": f"Limit of {limit} videos has been reached."}
                        )
                occurrences = {}

                if videos:
                    for v in videos:
                        occurrences[v] = occurrences.get(v, 0) + 1
                for video in user_videos:
                    # timestamp = date_to_timestamp(video["Date"])
                    tiktok_id = video_url_to_id(
                        video.get("Link", video.get("VideoLink"))
                    )
                    occurrences[tiktok_id] = occurrences.get(tiktok_id, 0) + 1
                unprocessed = []
                for key, value in occurrences.items():
                    if value == 1:
                        unprocessed.append(key)
                print(f"processing {len(unprocessed[:limit])}")
                task = process_videos.apply_async((unprocessed[:limit], user_id))
                res = task.id
                # limit to 3 videos for testing
            except Exception as e:
                print(e)
                res = None
                user_videos = []
    # Do something with user_videos
    return json.dumps({"result_id": res, "type": 1})


@flask_app.get("/api/v1/checkSubmit")
def task_result():
    result_id = request.args.get("result_id")
    t = int(request.args.get("type"))
    if t == 0 or t == 1:
        queue = CHECK_TYPES[t]
    else:
        return jsonify(
            {"status": "ERROR", "error_message": "Request type not supported"}
        )
    result = queue.AsyncResult(result_id)
    if result.ready():  # -Line 5
        # Task has completed
        if result.successful():  # -Line 6
            return {
                "ready": result.ready(),
                "successful": result.successful(),
                "value": result.result,  # -Line 7
            }
        else:
            # Task completed with an error
            return jsonify({"status": "ERROR", "error_message": str(result.result)})
    else:
        # Task is still pending
        return jsonify({"status": "Running"})


@flask_app.post("/api/v1/search")
def search():
    jwt = request.headers.get("Authorization")
    try:
        user_id = supabase.auth.get_user(jwt).user.id
    except Exception as e:
        print(e)
        return json.dumps(str(e))
    search_query = request.form["search"]
    k = request.form.get("k", 3)
    task = query.apply_async((search_query, user_id, k))
    res = task.id
    return json.dumps({"result_id": res, "type": 2})


if __name__ == "__main__":
    flask_app.run(debug=True)
else:
    gunicorn_app = flask_app
