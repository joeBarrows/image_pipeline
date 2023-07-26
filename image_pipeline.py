import argparse
import boto3
import json
import requests
from io import BytesIO
from PIL import Image, ImageFilter
from prefect import task, flow

from prefect_aws import AwsCredentials
from prefect.blocks.system import Secret


#Import all the enhancement filter from pillow
from PIL.ImageFilter import (
   BLUR, CONTOUR, DETAIL, EDGE_ENHANCE, EDGE_ENHANCE_MORE,
   EMBOSS, FIND_EDGES, SMOOTH, SMOOTH_MORE, SHARPEN
)

S3_BUCKET_NAME = "prefect-image-test"
IMAGE_DIR = "images/"

@task
def get_image_credentials():
    #url = "https://api.pexels.com/v1/search?query=soccer&per_page=20"
    url = "https://api.pexels.com/v1/"
    secret_block = Secret.load("pexel-api-key")

    # Access the stored secret
    api_key = secret_block.get()
    #api_key = "v1hPpoYGw0mSoHnWMwLC4roGGtXEr3Dm3d8qynfcjv99DMnVKs1xKGaE"
    return url, api_key

@task
def create_s3_client():
    """
    Task to create an S3 client using AWS credentials.
    """
    aws_credentials_block = AwsCredentials.load("jbtest")
    return aws_credentials_block.get_boto3_session().client("s3")

@task
def get_image_data(url, api_key, subject, count):
    search_url = f"{url}search?query={subject}&per_page={count}"
    headers = {"Authorization": api_key}
    response = requests.get(search_url, headers=headers)
    data = response.json()
    return data

@task
def download_image(image_url, image_file_basename, api_key):
    image_name = f"{image_file_basename}_original.jpg"
    headers = {"Authorization": api_key}
    response = requests.get(image_url, headers=headers)

    if response.status_code == 200:
        with open(image_name, "wb") as f:
            f.write(response.content)
        print("Image downloaded successfully.")
    else:
        print(f"Failed to download image. Status code: {response.status_code}")
    return image_name

@task
def filter_image(image_file_basename, image_filter):
    filter_instance = getattr(ImageFilter, image_filter)
    filtered_image_name = f"{image_file_basename}_filtered.jpg"
    original_image_name = f"{image_file_basename}_original.jpg"
    #Create image object
    img = Image.open(original_image_name)
    #Applying the blur filter
    filter_img = img.filter(filter_instance)
    filter_img.save(filtered_image_name)
    return filtered_image_name

@task
def create_metadata_record(photo_data, file_basename):
    filename = f"{file_basename}_data.json"
    desired_fields = ['id', 'width', 'height', 'photographer', 'photographer_url', 'alt']
    d = {key: photo_data[key] for key in desired_fields}
    d['src'] = photo_data['src']['original']
    with open(filename, "w") as outfile:
        json.dump(d, outfile)
    return filename

@task
def upload_to_s3(s3_client, filename):
    s3_client.upload_file(filename, S3_BUCKET_NAME, filename)

@flow
def image_data_pipeline(subject, image_filter, count, upload_to_s3):

    url, api_key = get_image_credentials()
    s3_client = create_s3_client()
    image_data = get_image_data(url, api_key, subject, count)

#TODO create a larger json string to print as a markdown artifact
#TODO sub object and sub dir should be subject
    for photo_data in image_data['photos']:
        image_url = photo_data['src']['original']
        alt_text = photo_data['alt']
        image_file_basename = alt_text.replace(" ","").strip()
        original_image_name = download_image(image_url, image_file_basename, api_key)
        filtered_image_name = filter_image(image_file_basename, image_filter)
        json_file = create_metadata_record(photo_data, image_file_basename)

        if upload_to_s3:
            for f in [original_image_name, filtered_image_name, json_file]:
                upload_to_s3(s3_client, f)


def main():
    parser = argparse.ArgumentParser(description='Image pipeline to pull down images, apply a filter and upload to s3.')
    parser.add_argument('--subject', required=True,
                        help='Subject of images. This argument is required.')

    parser.add_argument('--filter', choices=['BLUR', 'CONTOUR', 'DETAIL', 'EDGE_ENHANCE',
                                              'EDGE_ENHANCE_MORE', 'EMBOSS', 'FIND_EDGES',
                                              'SMOOTH', 'SMOOTH_MORE', 'SHARPEN'],
                        default='BLUR', help='Image filter to apply to images. Default: BLUR.')

    parser.add_argument('--image_count', type=int, default=10,
                        help='Number of images to process. Default: 10.')

    parser.add_argument('--upload_to_s3', action='store_true',
                        help='Enable this flag to upload the processed images to Amazon S3.')

    args = parser.parse_args()
    subject = args.subject
    image_filter = args.filter
    image_count = args.image_count
    upload_to_s3 = args.upload_to_s3
    image_data_pipeline(subject, image_filter, image_count, upload_to_s3)

if __name__ == "__main__":
    main()

