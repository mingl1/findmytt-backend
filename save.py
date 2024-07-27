#!/usr/bin/env python
import json
import os
from tqdm import tqdm
from TikTokApi import TikTokApi
from utilities import video_url_to_id
import asyncio
from sentence_transformers import SentenceTransformer
from pinecone.grpc import PineconeGRPC as Pinecone
from dotenv import load_dotenv
from supabase import create_client
import itertools
import time

load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

model = SentenceTransformer("Snowflake/snowflake-arctic-embed-s")

pc = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(os.getenv("INDEX_NAME"))


url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)


def chunks(iterable, batch_size=200):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def my_get_embedding(text):
    text = text.replace("\n", " ")
    text = "".join(filter(str.isalnum, text))
    return model.encode(text)


def embed_query(text):
    return model.encode(text, prompt_name="query")


async def get_video(ids):
    ret = []
    fails = []
    async with TikTokApi() as api:
        await api.create_sessions(
            num_sessions=1,
            sleep_after=3,
            suppress_resource_load_types=[
                "stylesheet",
                "image",
                "media",
                "font",
                "script",
                "eventsource",
                "websocket",
            ],
        )
        for i in tqdm(ids):
            try:
                print("getting video")
                video = api.video(url=f"https://www.tiktok.com/@/video/{i}/")
                print("getting video info")
                j = await video.info()
                txt = j["contents"][0]["desc"] + ";"
                for tag in j["contents"][0]["textExtra"]:
                    txt += tag["hashtagName"] + ";"
                    txt += j["author"]["nickname"] + ";" + j["music"]["title"]
                ret.append((i, txt))
                print("done with video sleeping")
                time.sleep(2)
                print("waking up")
            except Exception as e:
                print(e)
                fails.append(i)
                print("failed with video sleeping")
                time.sleep(2)
                print("waking up")
                continue
        return ret, fails


async def save_videos(data=None, location=None):
    mode = "Bookmarked"
    user_id = location
    if not data:
        mode = "Bookmarked"
        source = "user_data.json"
        location = "./videos"
        # Open JSON
        with open(source, encoding="utf8") as f:
            data = json.load(f)
        # Get list
        activity = data["Activity"]
        videos = (
            activity["Like List"]["ItemFavoriteList"]
            if mode == "liked"
            else activity["Favorite Videos"]["FavoriteVideoList"]
        )
        videos = [video_url_to_id(v.get("Link", v.get("VideoLink"))) for v in videos]
    else:
        location = "./" + location
        source = location + ".json"
        if not os.path.exists(location):
            os.makedirs(location)
        videos = data
    # Initialise tiktok API connector
    # api = TikTokApi.get_instance()
    # did = str(random.randint(10000, 999999999))

    # What videos are already accounted for?
    # videos = videos_to_check(videos, location, check_failures)
    print(videos)
    # Worth doing anything?
    if len(videos) == 0:
        print("Nothing new to download")
        return
    # if video id already in vector database,
    # simply associate already exisitng vector with user namespace
    # else
    # Save videos and metadata
    # documents_info = []
    # documents_id = []
    documents, failures = await get_video(videos)
    # thinking about shared namespace,
    # if 2 people favorite same video, they may have similar taste
    # so the pinecone database queries combination of their favorites
    # for now this wont be a thing
    document_ids = [i[0] for i in documents]
    if len(failures):
        print("Failed downloads:", len(failures))
    if len(documents):
        document_embeddings = model.encode([i[-1] for i in documents])
        documents = [
            {"id": i + "_" + user_id, "values": j}
            for i, j in zip(document_ids, document_embeddings)
        ]
        for ids_vectors_chunk in chunks(documents, batch_size=200):
            upsert_response = index.upsert(vectors=ids_vectors_chunk, namespace=user_id)
            print(upsert_response)
        # need to change to append instead of replace, currently it replaces videos
        # which is bad because document_ids only contain new videos, so old ones are wiped
        response = (
            supabase.table("user_videos")
            .upsert({"id": user_id, "videos": document_ids})
            .execute()
        )
        print(response)

    return failures


def query_helper(query_text, user_id, k):
    vec = model.encode(query_text, prompt_name="query")
    ret = index.query(namespace=user_id, vector=vec, top_k=int(k), include_values=False)
    print(ret)
    return [i["id"] for i in ret.matches]


if __name__ == "__main__":
    asyncio.run(save_videos())
# Any problems to report?
