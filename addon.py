import html
import json
import os
import os.path as path
import queue
import re
import sys
import urllib.parse
import urllib.request
from threading import Thread
from urllib.error import HTTPError

import xbmc
import xbmcaddon
import xbmcgui
import xbmcplugin
from xbmcvfs import translatePath

import cache

# Constants
CACHE_FILE_NAME = 'cache.json.gz'
CACHE_KEY_USER_PAGE = 'user_page'
CACHE_KEY_VIDEO_PAGE = 'video_page'
CACHE_KEY_EMBED_JSON = 'embed_json'


def show_gui(handle, url: str):
    page_number = 0
    video_page_urls = []
    fetch_next_user_page = True

    # Need to get an un-cached version
    for video_page_url in get_video_pages_from_user_urls(url + '?page={}'.format(1)):
        if video_page_url:
            if not cache.exists(CACHE_KEY_VIDEO_PAGE, video_page_url):
                # If a page is not in cache assume cache is old and rebuild the cache
                cache.clear(CACHE_KEY_USER_PAGE)
                break

    while fetch_next_user_page:
        # Make sure it's initialised on empty
        fetch_next_user_page = False
        page_number += 1

        # Request video page urls from user url
        for video_page_url in get_video_pages_from_user_urls_cached(url + '?page={}'.format(page_number)):
            if video_page_url:
                video_page_urls.append(video_page_url)
                fetch_next_user_page = True

    # Create progress bar
    rumble_user = url.split('/').pop()

    # Concurrency limit seems to be 99 concurrent connections on Rumble, better stay on a lower safe number (like 20)
    video_details = scrape_threaded(rumble_user, video_page_urls, 20)

    for video_detail in video_details:
        # Create list-item from embed-json
        add_list_item(handle, video_detail['embed'], video_detail['description'])

    # Finish list
    xbmcplugin.addSortMethod(handle, xbmcplugin.SORT_METHOD_DATEADDED)
    xbmcplugin.endOfDirectory(handle)


def get_addon_data_path(sub_path='') -> str:
    addon_data_path = 'special://profile/addon_data/plugin.video.blckbx-tv'

    if not path.exists(translatePath(addon_data_path)):
        os.makedirs(translatePath(addon_data_path))

    return translatePath(addon_data_path + sub_path)


def request_url(url: str) -> str:
    return fetch_url(url)


@cache.persist(CACHE_KEY_USER_PAGE)
def get_video_pages_from_user_urls_cached(url: str) -> list:
    return get_video_pages_from_user_urls(url)


def get_video_pages_from_user_urls(url: str) -> list:
    user_html = fetch_url(url)

    # Construct the base url (url's in the html are relative)
    url_parts = url.split('/')
    url_parts.pop()
    url_parts.pop()
    prefix = '/'.join(url_parts)

    video_parts = re.findall('class=video-item--a href=.+?>', user_html)

    video_page_urls = []
    for video_part in video_parts:
        video_page_url = video_part.rsplit('href=', 1)[-1]
        video_page_urls.append(prefix + video_page_url[:-1])

        xbmc.log('Found videopart: ' + prefix + video_page_url[:-1], xbmc.LOGDEBUG)

    return video_page_urls


@cache.persist(CACHE_KEY_VIDEO_PAGE)
def get_video_page(url: str) -> dict:
    # Download video page html
    video_html = request_url(url)

    # Find the part in the page with the embed-json
    video_embed_part = ''
    video_embed_part_match = re.search('"embedUrl":"https?://.+/embed/.+?"', video_html)
    if video_embed_part_match:
        video_embed_part = video_embed_part_match.group()

    # Try to parse the description
    description = ''
    description_list = re.findall('<p class="media-description">.+</p>', video_html)
    if description_list:
        # strip tags
        description = "\n\n".join(description_list) \
            .replace('<br>', "\n") \
            .replace('<p>', "\n\n")
        description = re.sub('<[^<]+?>', '', description)

        # replace html escaped characters with normal text
        description = html.unescape(description)

    # Extract url from part
    embed_url = video_embed_part.rsplit('"', 2)[-2]

    return {'description': description, 'embed_url': embed_url}


@cache.persist(CACHE_KEY_EMBED_JSON)
def get_json_from_embed_url(url: str) -> dict:
    # Download embed html
    embed_html = request_url(url)

    # Last part of the embed-url is the video-id
    video_id = url.rsplit('/', 2)[-2]

    # Find part in the html where the json is assigned
    embed_part = re.search('\["' + video_id + '"\]={.+?(?!;)};', embed_html).group()

    # Strip off all non-json stuff, remove escape-slashes and a non-standard function in the json
    embed_json_string = re.search('{.*}', embed_part).group() \
        .replace(',loaded:a()', '') \
        .replace(',loaded:d()', '') \
        .replace(',loaded:getTime()', '') \
        .replace('\\/', '/')

    # Parse json to dict
    return json.loads(embed_json_string)


def add_list_item(handle, embed: dict, description: str):
    # Get data from dict
    video_id = embed.get('vid')
    title = html.unescape(embed.get('title'))
    author_name = html.unescape(embed.get('author').get('name'))
    duration = embed.get('duration')
    pub_date = embed.get('pubDate')
    thumb_url = embed.get('i')
    subtitles = embed.get('cc')
    codec = 'mp4'
    streams = embed.get('ua').get(codec)

    # Search for best quality
    stream_max_height = 0
    stream_max_width = 0
    stream_size = 0
    stream_url = ''
    for stream_height, stream in streams.items():
        if stream_max_height < int(stream_height):
            stream_url = stream.get('url')
            stream_max_height = int(stream_height)
            stream_max_width = stream.get('meta').get('w')
            stream_size = stream.get('meta').get('size')

    # Find subtitles
    subtitle_list = fetch_subtitles(subtitles, str(video_id))

    # Parse date "pubDate": "2022-09-25T17:39:36+00:00",
    date, time_with_zone = str(pub_date).split('T', 1)
    year, month, day = date.split('-', 2)
    time = time_with_zone.split('+', 1)[0]

    # Format dates
    date = '{}.{}.{}'.format(day, month, year)
    aired = '{}-{}-{}'.format(year, month, day)
    date_added = '{}-{}-{} {}'.format(year, month, day, time)

    # Assign data to list-item
    li = xbmcgui.ListItem()
    li.setLabel(title)
    li.setInfo('video', {
        'size': stream_size,
        'date': date,
        'title': title,
        'plot': title + "\n\n" + description,
        'duration': duration,
        'aired': aired,
        'studio': author_name,
        'cast': [author_name],
        'dateadded': date_added,
    })

    li.addStreamInfo('video', {
        'codec': 'mpeg4',
        'width': stream_max_width,
        'height': stream_max_height,
        'duration': duration
    })

    li.setSubtitles(subtitle_list)
    li.setArt({
        'thumb': thumb_url,
        'poster': thumb_url
    })

    xbmcplugin.addDirectoryItem(handle, stream_url, li)


def fetch_url(url: str) -> str:
    xbmc.log('Request url: ' + url, xbmc.LOGDEBUG)
    body = ''

    # Get html
    try:
        with urllib.request.urlopen(url) as response:
            body = response.read().decode(response.headers.get_content_charset() or 'utf-8')
    except HTTPError as e:
        xbmc.log('Requesting url failed: {}'.format(url), xbmc.LOGDEBUG)

    return body


def fetch_subtitles(cc: dict, video_id: str) -> list:
    # Find subtitles
    subtitle_list = []

    if len(cc) > 0:
        for language_code, subtitle in cc.items():
            url = subtitle.get('path')
            subtitle_dir = get_addon_data_path('/subs/' + video_id)
            subtitle_path = subtitle_dir + '/' + language_code + '.srt'

            # If subtitle file does not exist download and save it
            # Subtitles need te be placed locally because the filename tells Kodi the language of the file
            if not path.isdir(subtitle_dir):
                os.makedirs(subtitle_dir)

            if not path.isfile(subtitle_path):
                subtitle_content = fetch_url(url)
                subtitle_file = open(subtitle_path, 'wb')
                subtitle_file.write(subtitle_content.encode('utf-8'))
                subtitle_file.close()

            # Add local path
            subtitle_list.append(subtitle_path)

    return subtitle_list


def scrape_threaded(title, addresses, no_workers):
    class Worker(Thread):
        def __init__(self, request_queue):
            Thread.__init__(self)
            self.queue = request_queue
            self.results = []

        def run(self):
            while True:
                video_page_url = self.queue.get()
                if video_page_url == "":
                    break

                # Get video-page data
                video_page = get_video_page(video_page_url)

                # Create list-item from embed-url
                embed_dict = get_json_from_embed_url(video_page['embed_url'])

                # Pre-load subtitles if available
                video_id = embed_dict.get('vid', '')
                subtitles = embed_dict.get('cc', '')
                if video_id and subtitles:
                    fetch_subtitles(subtitles, str(video_id))

                self.results.append({'description': video_page['description'], 'embed': embed_dict})
                self.queue.task_done()

    # Create queue and add addresses
    q = queue.Queue()
    for url in addresses:
        q.put(url)

    # Get queue size
    queue_size = q.qsize()

    # Workers keep working till they receive an empty string
    for _ in range(no_workers):
        q.put("")

    # Create workers and add tot the queue
    workers = []
    for _ in range(no_workers):
        worker = Worker(q)
        worker.start()
        workers.append(worker)

    p_dialog = None

    # Join workers to wait till they finished
    for worker in workers:
        # While works al active update the progress bar
        while worker.is_alive():
            xbmc.sleep(1000)

            # If there is still work after a second show the progress bar
            if q.qsize() > 0:
                if not p_dialog:
                    p_dialog = xbmcgui.DialogProgress()
                    p_dialog.create(title)

                queue_progress = (queue_size - q.qsize())
                percent = int(round(queue_progress / queue_size * 100))
                p_dialog.update(percent, "Bezig met laden van video's...")

                # If dialog is canceled do not keep session cache
                if p_dialog.iscanceled():
                    break

    # Combine results from all workers
    r = []
    for worker in workers:
        r.extend(worker.results)

    # Close progress bar
    if p_dialog:
        p_dialog.close()

    return r


# Parse arguments
base_url = sys.argv[0]
addon_handle = int(sys.argv[1])
args = urllib.parse.parse_qs(sys.argv[2][1:])

# Configure addon
addon = xbmcaddon.Addon()
if addon_handle != -1:
    xbmcplugin.setContent(addon_handle, 'videos')

# Read cache
cache_file_path = get_addon_data_path('/' + CACHE_FILE_NAME)
cache.read(cache_file_path)

# Show GUI, plugin should work with any Rumble user
show_gui(addon_handle, 'https://rumble.com/user/BLCKBX')

# Write cache
cache.write(cache_file_path)
