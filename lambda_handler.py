import os
import boto3
import logging
import datetime
import time
import random
import uuid
from io import BytesIO
from urllib.parse import urlparse
import zipfile
from moviepy.editor import VideoFileClip, ColorClip, CompositeVideoClip

# Setting up the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the S3 client
s3 = boto3.client('s3')
bucket_name = 'processed-videos-1'

def get_from_s3(url):
    logger.info('in download from s3')
    url_path = urlparse(url).path
    file_extension = os.path.splitext(url_path)[1]
    file_extension = file_extension.lstrip(".")
    unique_id = str(uuid.uuid4())
    file_path = f"{unique_id}.{file_extension}"
    bucket, key = get_bucket_key_from_url(url)
    s3.download_file(bucket_name, key, file_path)
    return file_path


def get_temp_directory():
    # Use Lambda's /tmp directory
    unique_id = str(uuid.uuid4())
    temp_dir = os.path.join("/tmp", unique_id)
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f'temp dir %s', temp_dir)
    return temp_dir


def get_bucket_key_from_url(url):
    parsed_url = urlparse(url)
    bucket = parsed_url.netloc.split('.')[0]
    key = parsed_url.path.lstrip('/')
    return bucket, key


def upload_to_s3(file_content, object_name):
    try:
        start_time = time.time()
        s3.upload_fileobj(file_content, bucket_name, object_name, ExtraArgs={'ACL':'public-read'})
        end_time = time.time()
        logger.info('file upload in %s seconds ', str(end_time-start_time))
        s3_bucket_url = f"https://{bucket_name}.s3.amazonaws.com"
        file_url = f"{s3_bucket_url}/{object_name}"
        logger.info('file upload in %s seconds ', str(end_time-start_time))
        return file_url
    except Exception as e:
        logger.error('error in uploading', exc_info=True)
        return None



def create_video1(video1_path, style):
    logger.info('in create video 1 function')
    # video1_path = get_from_s3(video1_path)
    if style == 'overlay':
        # logger.log('video1 path %s', video1_path)
        video1 = VideoFileClip(video1_path)
        print('after video1', video1)
        video1 = video1.resize(height=1920)
    else:
        shrink_height = int(1920 * 0.75)
        # logger.info(f'shrink height %d', shrink_height)
        video1 = VideoFileClip(video1_path)
        aspect_ratio = video1.w / video1.h
        new_width = 1080
        video1 = video1.resize((new_width, shrink_height))
    print('video1', video1.w, video1.h)
    if video1.w != 1080:
        video1 = video1.crop(x_center=video1.w / 2, width=1080)
    print('video1 dimensions', video1.w, video1.h)
    frame_rate1 = int(video1.fps)
    return video1, frame_rate1


def create_video2(video2_path):
    # logger.info('in create video2 function')
    # video2_path = get_from_s3(video2_path)
    video2 = VideoFileClip(video2_path)
    logger.info('video2', video2)
    video2_height_40 = int(0.4 * 1920)
    video2 = video2.resize(height=video2_height_40)
    if video2.w > 1080:
        crop_amount = (video2.w - 1080) / 2
        video2 = video2.crop(x1=crop_amount, x2=video2.w - crop_amount).without_audio()
    else:
        video2 = video2.resize(width=1080)

    return video2

def lambda_handler(event, context):
    url_list = []
    video1_path = event['video1_path']
    video2_path = event['video2_path']
    split_variations = event['split_variations']
    style = event['style']

    video1, frame_rate1 = create_video1(video1_path, style)
    video2 = create_video2(video2_path)
    for i in range(split_variations):
        start_time = random.uniform(0, video2.duration - video1.duration)
        video2_subclip = video2.subclip(start_time, start_time + video1.duration)
        y_pos = int(0.6 * 1920)
        video1_position = (0, 0)
        video2_position = (0, y_pos)

        empty_clip = ColorClip(size=(1080, 1920), color=[0, 0, 0], duration=frame_rate1)
        final_video = CompositeVideoClip([empty_clip.set_duration(video1.duration),
                                        video1.set_position(video1_position),
                                        video2_subclip.set_position(video2_position)])

        current_time = datetime.datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
        unique_id = str(uuid.uuid4())
        file_name = os.path.join("/tmp", f"{formatted_time}-{unique_id}.mp4")
        try:
            final_video.write_videofile(file_name, codec='libx264', audio_codec='aac')
        except Exception as e:
            logger.error('An error occurred while writing video file: %s', str(e))

        with open(file_name, 'rb') as f:
            file_content = BytesIO(f.read())
        file_content.seek(0)

        file_object = f"splits/{formatted_time}-{unique_id}.mp4"
        file_url = upload_to_s3(file_content, file_object)
        os.remove(file_name)
        url_list.append(file_url)

    unique_id = str(uuid.uuid4())
    zip_file_name = os.path.join("/tmp", f'split_variations_{unique_id}.zip')

    with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for i in range(len(url_list)):
            parsed_url = urlparse(url_list[i])
            key = parsed_url.path.lstrip('/')
            file_content = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read()
            zipf.writestr(f'split_{i}.mp4', file_content)

    zip_file_object = f"zip/{unique_id}.zip"
    with open(zip_file_name, 'rb') as zip_file:
        zip_file_content = BytesIO(zip_file.read())

    zip_file_content.seek(0)
    zip_file_url = upload_to_s3(zip_file_content, zip_file_object)
    os.remove(zip_file_name)

    return zip_file_url