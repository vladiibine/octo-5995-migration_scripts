__author__ = 'vardelean'
import sys
import traceback
import functools
import requests

import urllib2
import MySQLdb
import os
import oauth2 as oauth
import httplib2
import time
import json
import logging
from StringIO import StringIO
import imghdr

logger = logging.Logger(__name__)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    assert 'DJANGO_SETTINGS_MODULE' in os.environ, (
        "the DJANGO_SETTINGS_MODULE "
        "needs to be specified")

    METHOD_POST = 'POST'
    VIDEO_ASSETS_NS = 'video-assets'
    WEBOBJECTS_NS = 'webobjects'

    DJANGO_SETTINGS_MODULE = os.environ['DJANGO_SETTINGS_MODULE']

    package_name = DJANGO_SETTINGS_MODULE.rsplit('.', 1)[0]
    settings_module = __import__(DJANGO_SETTINGS_MODULE,
                                 fromlist=[package_name])

    S3_STORAGE_ENDPOINT = settings_module.S3_STORAGE_ENDPOINT
    ITS_ENDPOINT = settings_module.ITS_ENDPOINT
    ITS_CONSUMER_KEY = settings_module.ITS_CONSUMER_KEY
    ITS_CONSUMER_SECRET = settings_module.ITS_CONSUMER_SECRET
    DATABASE = settings_module.DATABASES['default']['NAME']
    DB_USER = settings_module.DATABASES['default']['USER']
    DB_USER_PW = settings_module.DATABASES['default'].get('PASSWORD', '')
    DB_HOST = settings_module.DATABASES['default']['HOST']
    DB_PORT = settings_module.DATABASES['default'].get('PORT', '')

    if not S3_STORAGE_ENDPOINT.endswith('/'):
        S3_STORAGE_ENDPOINT += '/'


def log_entry(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug("Entered %s" % func.func_name)
        return func(*args, **kwargs)

    return wrapper


def mkcon():
    """Creates a connection to the database
    """
    conn_settings = {'host': DB_HOST, 'user': DB_USER, 'passwd': DB_USER_PW,
                     'db': DATABASE}
    if DB_PORT:
        conn_settings['port'] = DB_PORT

    connection = MySQLdb.connect(**conn_settings)
    connection.autocommit(False)
    return connection


def get_obj_imgs_map(res):
    """Returns the complete mapping of WOs to video profiles and image URLs
    """
    wo_imgs_map = {}
    row = res.fetch_row()
    while row:
        profile_url_map = wo_imgs_map.get(row[0][0], {})
        profile_url_map[row[0][2]] = row[0][1]
        wo_imgs_map[row[0][0]] = profile_url_map
        row = res.fetch_row()
    return wo_imgs_map


def update_wo_image(con, cursor, updatable_wos):
    """Updates WebObjects
    """
    for wo_id, image in updatable_wos.items():
        # logger.debug(("WO id: %s" % wo_id) + ("image %s " % image))
        try:
            if image:
                affected = cursor.execute("""
                        UPDATE %s.core_webobject
                        SET image = %s
                        WHERE id = %s;
                          """, (DATABASE, str(image), str(wo_id)))
            else:
                affected = cursor.execute("""
                         UPDATE %s.core_webobject
                         SET image = ''
                         WHERE id = %s;
                           """, (DATABASE, str(wo_id),))
            logger.debug(
                ('WO id: %s' % wo_id) + (' affected rows: %s' % affected))
        except MySQLdb.MySQLError:
            #TODO - log the failure
            con.rollback()
        else:
            con.commit()


def update_vpage_image(con, cursor, updatable_vps):
    """Updates VideoPages
    """
    for vp_id, image in updatable_vps.items():
        try:
            if image:
                cursor.execute("""
                 UPDATE %s.videoportal_videopage
                 SET stack_image = %s
                 WHERE id = %s;
                """, (DATABASE, str(image), str(vp_id)))
            else:
                cursor.execute("""
                 UPDATE %s.videoportal_videopage
                 SET stack_image = ''
                 WHERE id = %s;
                """, (DATABASE, str(vp_id),))
        except:
            #TODO - log failure
            con.rollback()
        else:
            con.commit()


@log_entry
def copy_wo_its_imgs(con, cursor):
    """For all the WebObjects of type Video with ITS images, migrates those
        images

    SHOULD be ready for testing

    """
    con.query("""
            SELECT wo.id, vaif.ingested_url, vaif.profile_id
                FROM %s.core_webobject AS wo
                INNER JOIN %s.core_video AS video
                    ON wo.id = video.webobject_ptr_id
                INNER JOIN %s.videoingester_videoasset AS vasset
                    ON video.videoasset_guid = vasset.guid
                INNER JOIN %s.videoingester_videoassetimagefile AS vaif
                    ON vasset.id = vaif.video_asset_id
                WHERE webobject_type='Video'
                AND ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                    OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%'
                );
    """, (DATABASE, DATABASE, DATABASE, DATABASE,))
    res = con.store_result()
    wo_imgs_map = get_obj_imgs_map(res)
    updatable_wos = create_usable_imgs_map(wo_imgs_map)
    #logger.debug(str(updatable_wos))

    update_wo_image(con, cursor, updatable_wos)


def create_usable_imgs_map(obj_imgs_map):
    """Returns a map of WO Ids : New Image (url) to be used in the migration
    """
    updatable_objs = {}
    for obj_id, profile_url_map in obj_imgs_map.items():
        new_image = profile_url_map.get(10l, '')
        if not new_image:
            if profile_url_map.items():
                new_image = profile_url_map.popitem()[1]

        if new_image and (not new_image.startswith('http')):
            new_image = S3_STORAGE_ENDPOINT + new_image

        updatable_objs[obj_id] = new_image
    return updatable_objs


def build_request(url, consumer, method='POST'):
    params = {
        'oauth_version': "1.0",
        'oauth_nonce': oauth.generate_nonce(),
        'oauth_timestamp': int(time.time()),
        'oauth_consumer_key': consumer.key,
    }

    req = oauth.Request(method=method, url=url, parameters=params)
    req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), consumer, None)
    return req


def fetch_file(location, max_size=None):
    """
    Fetch the file contents from the provided location.

    Note that this is to be used only for small-sized files as it can eat up
    the server memory for large files.

    :param location: the URL from which to fetch the remote file.
    :param max_size: if the maximum size is provided the file download will be
                     interrupted and an IOError will be raised.
    :raises: IOError if the download fails
    :returns: the file contents
    """
    buff = StringIO()
    response = requests.get(location, stream=True, verify=False)

    read_so_far = 0
    for block in response.iter_content(1024):
        if not block:
            break

        read_so_far += len(block) / (1024.0 * 1024.0)
        if max_size and read_so_far > max_size:
            raise IOError(
                "The provided file has a size greater than the "
                "maximum allowed of %s MB" % max_size)

        buff.write(block)

    return buff.getvalue()


def upload_img_to_its(img_url, its_endpoint, namespace, its_cons_key,
                      its_cons_secret):
    try:
        oauth_consumer = oauth.Consumer(key=its_cons_key,
                                        secret=its_cons_secret)
        its_destination = (its_endpoint.rstrip('/') + '/' +
                           namespace.rstrip('/') + '/')

        request = build_request(its_destination, oauth_consumer, METHOD_POST)
        headers = {}
        try:
            image = fetch_file(img_url, 20)
        except IOError, err:
            msg = 'File too big. \nTaceback: %s' % get_last_traceback_str()
            raise UploadException(message=msg, previous=err)

        header_types = {'png': 'image/png', 'gif': 'image/gif',
                        'jpeg': 'image/jpeg'}
        file_obj = StringIO(image)
        filetype = imghdr.what(file_obj)
        logger.debug("The file type was %s" % filetype)
        headers['Content-Type'] = header_types.get(filetype, 'image')

        request_url = request.to_url()

        resp = requests.post(request_url, data=image, headers=headers)
        if resp.status_code == 201:
            try:
                ingested_url = json.loads(resp.text)['public_url']
                logger.debug("The obtained URL: %s" % ingested_url)
                return ingested_url
            except (TypeError, AttributeError), err:
                message = ('Invalid response type: \n %s' % resp.text +
                           '\nTraceback: %s ' % get_last_traceback_str()
                )
                raise UploadException(
                    message=(message),
                    previous=err)
            except KeyError, err:
                message = ('The public url was not returned' +
                           '\nTraceback %s' % get_last_traceback_str())
                raise UploadException(message=message
                                      , previous=err)
    except UploadException, err:
        previous_err = getattr(err, 'previous', None)
        prev_msg = getattr(previous_err, 'message', '')
        raise UploadException(
            message=(err.message + ' Previous msg:' + prev_msg))
    except Exception, err:
        message = ('Unhandled exception while uploading' +
                   '\nTraceback: %s' % get_last_traceback_str())
        raise UploadException(message=message, previous=err)


def migrate_from_result(con, cursor, res, update_func, namespace, obj_type):
    """Given the result set, the cursor and the specific update function,
        updates the given object with the specific img_url
    """
    errors = {}
    model_imgs_map = get_obj_imgs_map(res)
    updatable_objs = create_usable_imgs_map(model_imgs_map)
    updated_objs = {}
    objs_count = len(updatable_objs)

    for counter, item in enumerate(updatable_objs.items()):
        obj_id, img_url = item
        logger.debug("Processing %s out of " % counter + str(objs_count))
        try:
            if img_url:
                logger.debug("Object id: %s, img_url: %s" % (obj_id, img_url))
                its_url = upload_img_to_its(img_url, ITS_ENDPOINT,
                                            namespace,
                                            ITS_CONSUMER_KEY,
                                            ITS_CONSUMER_SECRET)
                logger.debug("The obtained ITS URL %s" % its_url)
                updated_objs[obj_id] = its_url
            else:
                updated_objs[obj_id] = ''
        except UploadException, e:
            logger.info(
                "ERROR: %s: %s - %s" % (obj_type, str(obj_id), str(e.message)))
            errors[obj_id] = {'img_url': img_url, 'error': e.message}

    update_func(con, cursor, updated_objs)

    if errors:
        raise UploadException(errors=errors)


@log_entry
def migrate_video_non_its_images(con, cursor):
    """
    This will get both valid URLs and relative links (though most likely not).
    Append to the relative links the S3_STORAGE_ENDPOINT.

    """
    con.query("""
            SELECT wo.id, vaif.ingested_url, vaif.profile_id
            FROM %s.core_webobject AS wo
            INNER JOIN %s.core_video AS video
                ON wo.id = video.webobject_ptr_id
            INNER JOIN %s.videoingester_videoasset AS vasset
                ON video.videoasset_guid = vasset.guid
            INNER JOIN %s.videoingester_videoassetimagefile AS vaif
                ON vasset.id = vaif.video_asset_id
            WHERE webobject_type='Video'
            AND NOT ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%'
            );
    """, (DATABASE, DATABASE, DATABASE, DATABASE,))
    res = con.store_result()

    try:
        migrate_from_result(con, cursor, res, update_wo_image, VIDEO_ASSETS_NS,
                            'WEBOBJECT')
    except UploadException, ex:
        # logger.warning("The following WebObjects had errors:")
        # logger.warning(ex.errors)
        pass


@log_entry
def migrate_non_vid_non_its(con, cursor):
    """All the images that don't belong to the ITS endpoints will be uploaded
    to the ITS_ENDPOINT specified in the DJANGO_SETTINGS_MODULE

    Does migration for WebObjects of type WebObject (non-Video)
    """
    #we fake the profile ID - We hardcode mezzanine - doesn't matter either way
    con.query("""
    SELECT wo.id, wo.image, 10
        FROM %s.core_webobject AS wo
        WHERE wo.webobject_type = 'WebObject'
        AND NOT (
                wo.image LIKE 'http://image.pbs.org%'
                OR wo.image LIKE 'http://image-staging.pbs.org%'
            )
        AND wo.image <> ''
    ;
    """, (DATABASE,))

    res = con.store_result()
    try:
        migrate_from_result(con, cursor, res, update_wo_image, WEBOBJECTS_NS,
                            'WEBOBJECT')
    except UploadException, ex:
        pass
        # logger.warning("The following WebObjects had errors:")
        # logger.warning(ex.errors)


@log_entry
def migrate_vpage_non_its_imgs(con, cursor):
    """For the videopages, copy the ITS image url from the related VideoAsset

        For those assets without an ITS url, ingest into ITS that url, and
        update the VideoPage to use the new ITS url.
    """
    con.query("""
        SELECT page.id, vaif.ingested_url, vaif.profile_id
        FROM %s.videoportal_videopage AS page

        INNER JOIN %s.videoingester_videoasset AS asset
            ON page.video_asset_id = asset.id

        INNER JOIN %s.videoingester_videoassetimagefile AS vaif
            ON asset.id = vaif.video_asset_id

        WHERE NOT ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                    OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%' )
        ;
    """, (DATABASE, DATABASE, DATABASE))

    res = con.store_result()

    try:
        migrate_from_result(con, cursor, res, update_vpage_image,
                            VIDEO_ASSETS_NS, "VIDEOPAGE")
    except UploadException, ex:
        # logger.warning("The following VideoPages had errors:")
        # logger.warning(ex.errors)
        pass


class UploadException(Exception):
    def __init__(self, previous=None, message=None, errors=None, *args,
                 **kwargs):
        super(UploadException, self).__init__(message, *args, **kwargs)
        self.previous = previous
        self.errors = errors


@log_entry
def erase_unavailable_vp_imgs(con, cursor):
    """Set as '' the `stack_image` property of VideoPages whose VideoAsset
        doesn't have an image attached
    """
    # We fake the image and profile id (doesn't matter either way)
    con.query("""
    select outerpage.id, '', 10
    from %s.videoportal_videopage as outerpage
    where outerpage.id not in
        (select ((page.id))
            from %s.videoportal_videopage as page

            inner join %s.videoingester_videoasset as asset
                on page.video_asset_id = asset.id

            inner join %s.videoingester_videoassetimagefile as vaif
                on asset.id = vaif.video_asset_id
        );
    """, (DATABASE, DATABASE, DATABASE, DATABASE,))

    res = con.store_result()
    vpage_imgs_map = get_obj_imgs_map(res)
    updatable_vpages = create_usable_imgs_map(vpage_imgs_map)
    update_vpage_image(con, cursor, updatable_vpages)


@log_entry
def migrate_vpage_its_imgs(con, cursor):
    con.query("""
        SELECT page.id, vaif.ingested_url, vaif.profile_id
        FROM %s.videoportal_videopage AS page

        INNER JOIN %s.videoingester_videoasset AS asset
            ON page.video_asset_id = asset.id

        INNER JOIN %s.videoingester_videoassetimagefile AS vaif
            ON asset.id = vaif.video_asset_id

        WHERE ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                    OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%' )
        ;
    """, (DATABASE, DATABASE, DATABASE))
    res = con.store_result()

    vpage_imgs_map = get_obj_imgs_map(res)
    updatable_vpages = create_usable_imgs_map(vpage_imgs_map)
    update_vpage_image(con, cursor, updatable_vpages)


def get_last_traceback_str():
    sys_last_traceback = getattr(sys, 'last_traceback', '')
    if sys_last_traceback:
        return str(traceback.format_tb(sys_last_traceback))
    else:
        return ''


@log_entry
def erase_wo_img_4_invalid_asset(con, cursor):
    """There may be WebObjects of type Video, who point to an invalid
        VideoAsset (invalid guid).

        For all these WebObjects, clear their `image` field.
    """
    con.query("""
        SELECT wo.id, '', 10
            FROM %s.core_webobject AS wo
            INNER JOIN %s.core_video AS video
                ON wo.id = video.webobject_ptr_id
            LEFT JOIN %s.videoingester_videoasset AS vasset
                ON video.videoasset_guid = vasset.guid
            WHERE wo.image <> ''
            AND wo.image NOT LIKE 'http://image-staging.pbs.org%'
            AND wo.image NOT LIKE 'http://image.pbs.org%'
            AND vasset.id IS NULL
            ;
    """, (DATABASE, DATABASE, DATABASE))
    res = con.store_result()
    wo_imgs_map = get_obj_imgs_map(res)
    updatable_wos = create_usable_imgs_map(wo_imgs_map)
    update_wo_image(con, cursor, updatable_wos)


if __name__ == '__main__':
    con = mkcon()
    cursor = con.cursor()

    try:
        copy_wo_its_imgs(con, cursor)

        migrate_video_non_its_images(con, cursor)

        migrate_non_vid_non_its(con, cursor)

        erase_wo_img_4_invalid_asset(con, cursor)

        migrate_vpage_its_imgs(con, cursor)

        migrate_vpage_non_its_imgs(con, cursor)

        erase_unavailable_vp_imgs(con, cursor)

    except Exception, e:
        logger.error('Errors were not caught at the topmost level')
        logger.error(str(e))
        logger.error('Traceback:' % get_last_traceback_str())

    con.close()
