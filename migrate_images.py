__author__ = 'vardelean'

import functools
import requests
from StringIO import StringIO

import urllib2
import MySQLdb
import os
import oauth2 as oauth
import httplib2
import time
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    assert 'DJANGO_SETTINGS_MODULE' in os.environ, (
        "the DJANGO_SETTINGS_MODULE "
        "needs to be specified")

    METHOD_POST = 'POST'
    DJANGO_SETTINGS_MODULE = os.environ['DJANGO_SETTINGS_MODULE']

    package_name = DJANGO_SETTINGS_MODULE.rsplit('.', 1)[0]
    settings_module = __import__(DJANGO_SETTINGS_MODULE,
                                 fromlist=[package_name])

    S3_STORAGE_ENDPOINT = settings_module.S3_STORAGE_ENDPOINT
    ITS_ENDPOINT = settings_module.ITS_ENDPOINT
    ITS_CONSUMER_KEY = settings_module.ITS_CONSUMER_KEY
    ITS_CONSUMER_SECRET = settings_module.ITS_CONSUMER_SECRET

    if not S3_STORAGE_ENDPOINT.endswith('/'):
        S3_STORAGE_ENDPOINT += '/'


def log_entry(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Entered %s" % func.func_name)
        return func(*args, **kwargs)

    return wrapper


def mkcon():
    """Mock this better.... wtf!!!
    """
    connection = MySQLdb.connect('localhost', 'merlin_user', '')
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


def update_wo_image(cursor, updatable_wos):
    """Updates WebObjects
    """
    for wo_id, image in updatable_wos.items():
        # logger.debug(("WO id: %s" % wo_id) + ("image %s " % image))
        if image:
            #TODO: get rid of 'merlin' - hardcoded database name
            affected = cursor.execute("""
                    UPDATE merlin.core_webobject
                    SET image = %s
                    WHERE id = %s;
                      """, (str(image), str(wo_id)))
        else:
            #TODO: get rid of 'merlin' - hardcoded database name
            affected = cursor.execute("""
                     UPDATE merlin.core_webobject
                     SET image = ''
                     WHERE id = %s;
                       """, (str(wo_id),))
            logger.debug(
                ('WO id: %s' % wo_id) + (' affected rows: %s' % affected))


def update_vpage_image(cursor, updatable_vps):
    """Updates VideoPages
    """
    for vp_id, image in updatable_vps.items():
        if image:
            #TODO: get rid of 'merlin' - hardcoded database name
            cursor.execute("""
             UPDATE merlin.videoportal_videopage
             SET stack_image = %s
             WHERE id = %s;
            """, (str(image), str(vp_id)))
        else:
            #TODO: get rid of 'merlin' - hardcoded database name
            cursor.execute("""
             UPDATE merlin.videoportal_videopage
             SET stack_image = ''
             WHERE id = %s;
            """, (str(vp_id),))


@log_entry
def copy_wo_its_imgs(con, cursor):
    """For all the WebObjects of type Video with ITS images, migrates those
        images

    SHOULD be ready for testing

    """
    con.query("""
            SELECT wo.id, vaif.ingested_url, vaif.profile_id
                FROM merlin.core_webobject AS wo
                INNER JOIN merlin.core_video AS video
                    ON wo.id = video.webobject_ptr_id
                INNER JOIN merlin.videoingester_videoasset AS vasset
                    ON video.videoasset_guid = vasset.guid
                INNER JOIN merlin.videoingester_videoassetimagefile AS vaif
                    ON vasset.id = vaif.video_asset_id
                WHERE webobject_type='Video'
                AND ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                    OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%'
                );
    """)
    res = con.store_result()
    wo_imgs_map = get_obj_imgs_map(res)
    #logger.debug(str(wo_imgs_map))
    updatable_wos = create_usable_imgs_map(wo_imgs_map)
    #logger.debug(str(updatable_wos))

    update_wo_image(cursor, updatable_wos)


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


def upload_img_to_its(img_url, its_endpoint, its_cons_key, its_cons_secret):
    oauth_consumer = oauth.Consumer(key=its_cons_key,
                                    secret=its_cons_secret)
    request = build_request(its_endpoint, oauth_consumer, METHOD_POST)
    headers = {'Content-Type': 'image/png'}  # TODO - dunno if this matters :|

    try:
        image = fetch_file(img_url, 20)
    except IOError:
        raise UploadException(message='file too big')

    request_url = request.to_url()

    resp = requests.post(request_url, data=image, headers=headers)
    if resp.status_code == 201:
        try:
            ingested_url = json.loads(resp.text)['public_url']
            logger.info("The obtained URL: %s" % ingested_url)
            return ingested_url
        except (TypeError, AttributeError):
            raise UploadException(
                message=('invalid response type: \n %s' % resp.text))
        except KeyError:
            raise UploadException(message='the public url was not returned')


def deprecated_upload_img_to_its(img_url, its_endpoint, its_cons_key,
                                 its_cons_secret):
    """Uploads the specified image, from the given url into ITS, and returns
    the link for that image

    :param its_cons_secret: its consumer secret key (from the settings module)
    :param its_cons_key: the consumer key (from the settings module)
    :param its_endpoint: the ITS endpoint (from the settings file)
    :param img_url: the source url of the image
    :return The URL of the image, posted in ITS
    """
    oauth_consumer = oauth.Consumer(key=its_cons_key,
                                    secret=its_cons_secret)

    try:
        image_stream = urllib2.urlopen(img_url)
    except (urllib2.HTTPError, AttributeError), e:
        raise UploadException(previous=e)

    request = build_request(its_endpoint, oauth_consumer, METHOD_POST)

    conn = httplib2.Http()
    headers = {'Content-Type': 'image/png'}

    image = image_stream.read()
    request_url = request.to_url()

    try:
        # logger.debug(image)
        logger.debug("investigate the 'image' name")
        resp, content = conn.request(
            request_url,
            METHOD_POST,
            body=image,
            headers=headers)
        logger.warning('ping')

    except Exception as e:
        logger.warning("Connection problem")
        logger.warning("Original image URL: %s" % img_url)
        logger.warning("Type of img_url var: %s" % type(img_url))
        logger.warning("Error type %s" % type(e))
        logging.exception("exception occured")
        raise e

    if resp['status'] == 201:
        json_content = json.loads(content)
        return json_content['public_url']
    else:
        raise UploadException(message='Uploading failed for %s' % img_url)


def migrate_from_result(cursor, res, update_func):
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
        logger.info("Processing %s out of " % counter + str(objs_count))
        try:
            if img_url:
                logger.info("Object id: %s, img_url: %s" % (obj_id, img_url))
                updated_objs[obj_id] = upload_img_to_its(
                    img_url,
                    ITS_ENDPOINT,
                    ITS_CONSUMER_KEY,
                    ITS_CONSUMER_SECRET)
            else:
                updated_objs[obj_id] = ''
        except UploadException, e:
            errors[obj_id] = {'img_url': img_url, 'error': e.message}

    update_func(cursor, updated_objs)

    if errors:
        logger.info("Errors: ")
        logger.info(errors)
        raise UploadException(errors=errors)


@log_entry
def migrate_video_non_its_images(con, cursor):
    """
    This will get both valid URLs and relative links (though most likely not).
    Append to the relative links the S3_STORAGE_ENDPOINT.

    """
    con.query("""
            SELECT wo.id, vaif.ingested_url, vaif.profile_id
            FROM merlin.core_webobject AS wo
            INNER JOIN merlin.core_video AS video
                ON wo.id = video.webobject_ptr_id
            INNER JOIN merlin.videoingester_videoasset AS vasset
                ON video.videoasset_guid = vasset.guid
            INNER JOIN merlin.videoingester_videoassetimagefile AS vaif
                ON vasset.id = vaif.video_asset_id
            WHERE webobject_type='Video'
            AND NOT ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%'
            );
    """)
    res = con.store_result()

    try:
        migrate_from_result(cursor, res, update_wo_image)
    except UploadException, ex:
        logger.warning("The following WebObjects had errors:")
        logger.warning(ex.errors)


def migrate_non_vid_its_images():
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
        FROM merlin.core_webobject AS wo
        WHERE wo.webobject_type = 'WebObject'
        AND NOT (
                wo.image LIKE 'http://image.pbs.org%'
                OR wo.image LIKE 'http://image-staging.pbs.org%'
            )
        AND wo.image <> ''
    ;
    """)

    res = con.store_result()
    try:
        migrate_from_result(cursor, res, update_wo_image)
    except UploadException, ex:
        logger.warning("The following WebObjects had errors:")
        logger.warning(ex.errors)


@log_entry
def migrate_vpage_non_its_imgs(con, cursor):
    """For the videopages, copy the ITS image url from the related VideoAsset

        For those assets without an ITS url, ingest into ITS that url, and
        update the VideoPage to use the new ITS url.
    """
    con.query("""
        SELECT page.id, vaif.ingested_url, vaif.profile_id
        FROM merlin.videoportal_videopage AS page

        INNER JOIN merlin.videoingester_videoasset AS asset
            ON page.video_asset_id = asset.id

        INNER JOIN merlin.videoingester_videoassetimagefile AS vaif
            ON asset.id = vaif.video_asset_id

        WHERE NOT ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                    OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%' )
        ;
    """)

    res = con.store_result()

    try:
        migrate_from_result(cursor, res, update_vpage_image)
    except UploadException, ex:
        logger.warning("The following VideoPages had errors:")
        logger.warning(ex.errors)


@log_entry
def copy_non_vid_its(con, cursor):
    """
    For the WeObjects (webobject_type = 'WebObject') with ITS images,
        don't do anything
    """

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
    from merlin.videoportal_videopage as outerpage
    where outerpage.id not in
        (select ((page.id))
            from merlin.videoportal_videopage as page

            inner join merlin.videoingester_videoasset as asset
                on page.video_asset_id = asset.id

            inner join merlin.videoingester_videoassetimagefile as vaif
                on asset.id = vaif.video_asset_id
        );
    """)

    res = con.store_result()
    vpage_imgs_map = get_obj_imgs_map(res)
    updatable_vpages = create_usable_imgs_map(vpage_imgs_map)
    update_vpage_image(cursor, updatable_vpages)


@log_entry
def migrate_vpage_its_imgs(con, cursor):
    con.query("""
        SELECT page.id, vaif.ingested_url, vaif.profile_id
        FROM merlin.videoportal_videopage AS page

        INNER JOIN merlin.videoingester_videoasset AS asset
            ON page.video_asset_id = asset.id

        INNER JOIN merlin.videoingester_videoassetimagefile AS vaif
            ON asset.id = vaif.video_asset_id

        WHERE ( vaif.ingested_url LIKE 'http://image.pbs.org%'
                    OR vaif.ingested_url LIKE 'http://image-staging.pbs.org%' )
        ;
    """)
    res = con.store_result()

    vpage_imgs_map = get_obj_imgs_map(res)
    updatable_vpages = create_usable_imgs_map(vpage_imgs_map)
    update_vpage_image(cursor, updatable_vpages)


@log_entry
def erase_wo_img_4_invalid_asset(con, cursor):
    """There may be WebObjects of type Video, who point to an invalid
        VideoAsset (invalid guid).

        For all these WebObjects, clear their `image` field.
    """
    con.query("""
        select
            wo.id, '', 10
            from merlin.core_webobject as wo
            inner join merlin.core_video as video
                on wo.id = video.webobject_ptr_id
            left join merlin.videoingester_videoasset as vasset
                on video.videoasset_guid = vasset.guid
            where wo.image <> ''
            and wo.image not like 'http://image-staging.pbs.org%'
            and wo.image not like 'http://image.pbs.org%'
            and vasset.id is NULL
            ;
    """)
    res = con.store_result()
    wo_imgs_map = get_obj_imgs_map(res)
    updatable_wos = create_usable_imgs_map(wo_imgs_map)
    update_wo_image(cursor, updatable_wos)


if __name__ == '__main__':
    con = mkcon()  # TODO!!!! do this better, rofl!
    cursor = con.cursor()

    try:
        copy_wo_its_imgs(con, cursor)

        migrate_video_non_its_images(con, cursor)

        migrate_non_vid_non_its(con, cursor)

        erase_wo_img_4_invalid_asset(con, cursor)

        copy_non_vid_its(con, cursor)

        migrate_vpage_its_imgs(con, cursor)

        migrate_vpage_non_its_imgs(con, cursor)

        erase_unavailable_vp_imgs(con, cursor)

    except Exception, e:
        # con.rollback()
        logger.warning('Errors were not caught at the topmost level')
        logger.warning(str(e))
        pass
    else:
        # con.commit()
        pass

    con.commit()
    con.close()
