import gzip
import html
import json
import os
import os.path as path
import re
import sys
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError
import xbmc
import xbmcaddon
import xbmcgui
import xbmcplugin
from xbmcvfs import translatePath


# TODO: refactor plugin to concurrent connections:
#  https://towardsdatascience.com/parallel-web-requests-in-python-4d30cc7b8989


def show_gui(handle, url: str):
    page_number = 0
    video_page_urls = []
    page_has_content = True

    # Do not keep session cache longer than one hour
    clear_session_cache()

    while page_has_content:
        # Make sure it's initialised on empty
        page_has_content = False
        page_number += 1

        # Request video page urls from user url
        for video_page_url_page in get_video_pages_from_user_url(url + '?page={}'.format(page_number)):
            if video_page_url_page:
                video_page_urls.append(video_page_url_page)
                page_has_content = True

    # Create progress bar
    rumble_user = url.split('/').pop()
    p_dialog = xbmcgui.DialogProgress()
    p_dialog.create(rumble_user)
    url_number = 0

    for video_page_url in video_page_urls:
        # Get embed-url from page-url
        embed_url = get_embed_url_from_video_page(video_page_url)

        # Create list-item from embed-url
        embed_dict = get_json_from_embed_url(embed_url)

        url_number += 1
        percent = int(round(url_number / len(video_page_urls) * 100))
        p_dialog.update(percent, html.unescape(embed_dict.get('title')))

        # Create list-item from embed-json
        add_list_item(handle, embed_dict)

        # If dialog is canceled do not keep session cache
        if p_dialog.iscanceled():
            clear_session_cache(True)
            break

    # Close progress bar
    p_dialog.close()

    # Finish list
    xbmcplugin.endOfDirectory(handle)


def get_addon_data_path(sub_path='') -> str:
    addon_data_path = 'special://profile/addon_data/plugin.video.blckbx-tv'

    if not path.exists(translatePath(addon_data_path)):
        os.makedirs(translatePath(addon_data_path))

    return translatePath(addon_data_path + sub_path)


def request_url(url: str) -> str:
    return fetch_url(url)


def persist_to_file(file_name) -> any:
    addon_data_file_name = get_addon_data_path('/' + file_name)

    def decorator(original_func) -> any:
        try:
            # Decode gzip and load json
            gzip_file_readonly = gzip.open(addon_data_file_name)
            cache = json.loads(gzip_file_readonly.read())
            gzip_file_readonly.close()

        except (IOError, ValueError):
            cache = {}

        def new_func(param) -> str:

            if param in cache:
                xbmc.log('Cache hit {}: {}'.format(file_name, param), xbmc.LOGDEBUG)
            else:
                cache[param] = original_func(param)

                # Dump json to gzip
                gzip_file_write = gzip.open(addon_data_file_name, 'wb')
                gzip_file_write.write(json.dumps(cache).encode('utf-8'))
                gzip_file_write.close()
            return cache[param]

        return new_func

    return decorator


@persist_to_file('session.dat.gz')
def get_video_pages_from_user_url(url: str) -> list:
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


@persist_to_file('video_page_to_embed_url_mapping_cache.dat.gz')
def get_embed_url_from_video_page(url: str) -> str:
    # Download video page html
    video_html = request_url(url)

    # Find the part in the page with the embed-json
    video_embed_part = re.search('"embedUrl":"https?://.+/embed/.+?"', video_html).group()

    # Extract url from part
    embed_url = video_embed_part.rsplit('"', 2)[-2]

    return embed_url


@persist_to_file('embed_json_cache.dat.gz')
def get_json_from_embed_url(url: str) -> dict:
    # Download embed html
    embed_html = request_url(url)

    # Last part of the embed-url is the video-id
    video_id = url.rsplit('/', 2)[-2]

    # Find part in the html where the json is assigned
    embed_part = re.search('\["' + video_id + '"\]={.+?(?!;)};', embed_html).group()

    # Strip off all non-json stuff, remove escape-slashes and a non standard function in the json
    embed_json_string = re.search('{.*}', embed_part).group() \
        .replace(',loaded:a()', '') \
        .replace(',loaded:d()', '') \
        .replace(',loaded:getTime()', '') \
        .replace('\\/', '/')

    # Parse json to dict
    return json.loads(embed_json_string)


def add_list_item(handle, embed_dict: dict):
    # Get data from dict
    video_id = embed_dict.get('vid')
    title = html.unescape(embed_dict.get('title'))
    author_name = html.unescape(embed_dict.get('author').get('name'))
    duration = embed_dict.get('duration')
    pub_date = embed_dict.get('pubDate')
    thumb_url = embed_dict.get('i')
    subtitles = embed_dict.get('cc')
    codec = 'mp4'
    streams = embed_dict.get('ua').get(codec)

    # Search for best quality
    stream_max_height = 0
    stream_max_width = 0
    stream_url = ''
    for stream_height, stream in streams.items():
        if stream_max_height < int(stream_height):
            stream_url = stream.get('url')
            stream_max_height = int(stream_height)
            stream_max_width = stream.get('meta').get('w')

    # Find subtitles
    subtitle_list = fetch_subtitles(subtitles, str(video_id))

    # Assign data to list-item
    li = xbmcgui.ListItem()
    li.setLabel(title)
    li.setInfo('video', {
        'title': title,
        'plot': title,
        'plotoutline': title,
        'duration': duration,
        'aired': pub_date,
        'studio': author_name,
        'cast': [author_name]
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


def is_file_older_than_hours(file, minutes) -> bool:
    xbmc.log('Checking cache file: {}'.format(file), xbmc.LOGDEBUG)
    if not path.exists(file):
        return False

    file_time = path.getmtime(file)

    return (time.time() - file_time) / 60 > minutes


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


def clear_session_cache(force: bool = False):
    session_cache_path = get_addon_data_path('/session.dat.gz')

    if path.isfile(session_cache_path):
        if force or is_file_older_than_hours(session_cache_path, 60):
            os.unlink(session_cache_path)


# Parse arguments
base_url = sys.argv[0]
addon_handle = int(sys.argv[1])
args = urllib.parse.parse_qs(sys.argv[2][1:])

# Configure addon
addon = xbmcaddon.Addon()
if addon_handle != -1:
    xbmcplugin.setContent(addon_handle, 'videos')

# Show GUI, plugin should work with any Rumble user
show_gui(addon_handle, 'https://rumble.com/user/BLCKBX')
