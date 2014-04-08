__author__ = 'vardelean'

import urllib2
import MySQLdb
import os
import oauth2 as oauth
import httplib2
import time
import json

# import httplib
# import sys
# from optparse import OptionParser


assert 'DJANGO_SETTINGS_MODULE' in os.environ, ("the DJANGO_SETTINGS_MODULE "
                                                "needs to be specified")

METHOD_POST = 'POST'
DJANGO_SETTINGS_MODULE = os.environ['DJANGO_SETTINGS_MODULE']

package_name = DJANGO_SETTINGS_MODULE.rsplit('.', 1)[0]
settings_module = __import__(DJANGO_SETTINGS_MODULE, fromlist=[package_name])

S3_STORAGE_ENDPOINT = settings_module.S3_STORAGE_ENDPOINT
ITS_ENDPOINT = settings_module.ITS_ENDPOINT
ITS_CONSUMER_KEY = settings_module.ITS_CONSUMER_KEY
ITS_CONSUMER_SECRET = settings_module.ITS_CONSUMER_SECRET

if not S3_STORAGE_ENDPOINT.endswith('/'):
    S3_STORAGE_ENDPOINT += '/'


def mkcon():
    """Mock this better.... wtf!!!
    """
    connection = MySQLdb.connect('localhost', 'merlin_user', '')
    connection.autocommit(False)
    return connection


#1.Migrate WebObject images, with webobject_type='Video'
#-copy image from VideoAssetImageFile.ingested_url (with) to WebOjbect
# .image <--- For ITSurls


def get_webobj_imgs_map(res):
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
    for wo_id, image in updatable_wos:
        if image:
            cursor.execute("""
                    UPDATE merlin.core_webobject
                    SET image = %s
                    WHERE id = %s;
                      """, (image, wo_id))
        else:
            cursor.execute("""
                     UPDATE merlin.core_webobject
                     SET image = NULL
                     WHERE id = %s;
                       """, (wo_id,))


def copy_videoasset_its_imgs(con, cursor):
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
    wo_imgs_map = get_webobj_imgs_map(res)
    updatable_wos = create_usable_imgs_map(wo_imgs_map)
    update_wo_image(cursor, updatable_wos)


def create_usable_imgs_map(wo_imgs_map):
    """Returns a map of WO Ids : New Image (url) to be used in the migration
    """
    updatable_wos = {}
    for wo_id, profile_url_map in wo_imgs_map:
        new_image = profile_url_map.get(10l, None)
        if not new_image and profile_url_map.items():
            new_image = profile_url_map.popitem()[1]
        else:
            new_image = None

        if new_image and (not new_image.startswith('http')):
            new_image = S3_STORAGE_ENDPOINT + new_image

        updatable_wos[wo_id] = new_image
    return updatable_wos


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


def upload_img_to_its(img_url, its_endpoint, its_cons_key, its_cons_secret):
    """Uploads the specified image, from the given url into ITS, and returns
    the link for that image

    :return The URL of the image, posted in ITS
    """
    oauth_consumer = oauth.Consumer(key=its_cons_key,
                                    secret=its_cons_secret)

    try:
        image_stream = urllib2.urlopen(img_url)
    except urllib2.HTTPError, e:
        raise UploadException(e)

    request = build_request(its_endpoint, oauth_consumer, METHOD_POST)

    conn = httplib2.Http()
    headers = {'Content-Type': 'image/png'}
    resp, content = conn.request(
        request.to_url(),
        METHOD_POST,
        body=image_stream.read(),
        headers=headers)

    if resp['status'] == 201:
        json_content = json.loads(content)
        return json_content['public_url']
    else:
        raise UploadException('Uploading failed for %s' % img_url)


def migrate_from_result(cursor, res):
    """Given the result set, and the cursor, updates the
    """
    errors = {}
    wo_imgs_map = get_webobj_imgs_map(res)
    updatable_wos = create_usable_imgs_map(wo_imgs_map)
    updated_wos = {}
    for wo_id, img_url in updatable_wos:
        try:
            updated_wos[wo_id] = upload_img_to_its(img_url, ITS_ENDPOINT,
                                                   ITS_CONSUMER_KEY,
                                                   ITS_CONSUMER_SECRET)
        except UploadException:
            errors[wo_id] = img_url
    if not errors:
        update_wo_image(cursor, updated_wos)
    else:
        raise Exception('')


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

    migrate_from_result(cursor, res)


def migrate_non_vid_its_images():
    pass


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
    migrate_from_result(cursor, res)


def copy_non_vid_its(con, cursor):
    """
    For the WeObjects (webobject_type = 'WebObject') with ITS images,
        either  
    """
    pass


class UploadException(Exception):
    pass





if __name__ == '__main__':

    con = mkcon()  #!!!! do this better, rofl!
    cursor = con.cursor()

    try:
        copy_videoasset_its_imgs(con, cursor)

        migrate_video_non_its_images(con, cursor)

        migrate_non_vid_non_its(con, cursor)

        copy_non_vid_its(con, cursor)

    except Exception, e:
        con.rollback()
    else:
        con.commit()

    con.close()