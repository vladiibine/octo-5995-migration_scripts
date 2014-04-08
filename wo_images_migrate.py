__author__ = 'vardelean'
import MySQLdb
import os
import oauth2 as oauth
import httplib2

# import httplib
# import time
# import sys
# from optparse import OptionParser


assert 'DJANGO_SETTINGS_MODULE' in os.environ, ("the DJANGO_SETTINGS_MODULE "
                                                "needs to be specified")

DJANGO_SETTINGS_MODULE = os.environ['DJANGO_SETTINGS_MODULE']

package_name = DJANGO_SETTINGS_MODULE.rsplit('.', 1)[0]
settings_module = __import__(DJANGO_SETTINGS_MODULE, fromlist=[package_name])
S3_STORAGE_ENDPOINT = settings_module.S3_STORAGE_ENDPOINT

if not S3_STORAGE_ENDPOINT.endswith('/'):
    S3_STORAGE_ENDPOINT += '/'


def mkcon():
    """Mock this better.... wtf!!!
    """
    return MySQLdb.connect('localhost', 'merlin_user', '')


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


def migrate_videoasset_its_imgs():
    """For all the WebObjects of type Video with ITS images, migrates those
        images

    Ignore duplicates (same image multiple times = ok

    """
    con = mkcon()
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
    cursor = con.cursor()
    updatable_wos = create_updatable_wos_map(wo_imgs_map)
    update_wo_image(cursor, updatable_wos)
    con.commit()
    con.close()


def create_updatable_wos_map(wo_imgs_map):
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


def build_request(url, consumer, method='GET'):
    params = {
        'oauth_version': "1.0",
        'oauth_nonce': oauth.generate_nonce(),
        'oauth_timestamp': int(time.time()),
        'oauth_consumer_key': consumer.key,
    }

    req = oauth.Request(method=method, url=url, parameters=params)
    req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), consumer, None)
    return req


def get_its_link(url):
    """Uploads the specified image, from the given url into ITS, and returns
    the link for that image

    """
    #Hardcode authentication
    oauth_consumer = oauth.Consumer(
        key='MERLIN-EF5855AD-34B7-48EF-BA1A-4974A9810C1C',
        secret='6EF0D4A9-1AF1-4A7E-BD6E-6F109FDB4877')

    conn = httplib2.Http()

    request = build_request('')

    pass


def migrate_video_non_its_images():
    """
    This will get both valid URLs and relative links (though most likely not).
    Append to the relative links the S3_STORAGE_ENDPOINT.

    S3_STORAGE_ENDPOINT

    For relative links, migrate them all
    """
    con = mkcon()
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

    wo_imgs_map = get_webobj_imgs_map(res)

    updatable_wos = create_updatable_wos_map(wo_imgs_map)
    #TODO: here's where we must process every image im the wo_imgs_map
    #...meaning upload to ITS and get the ITS link
    updated_wos = {}
    for wo_id, img_url in updatable_wos:
        updated_wos[wo_id] = get_its_link(img_url)

    update_wo_image(con.cursor(), updated_wos)
    con.commit()
    con.close()


def migrate_non_vid_its_images():
    pass


#TODO: the urls of form "webobjects/asdfasdfasdf.asdf" remain the same


#TODO: the images from weird sites are to be ingested into ITS, and we keep the
# url

def ingest_valid_imgs_into_ITS():
    """All the images that don't belong to the ITS endpoints will be uploaded
    to the ITS_ENDPOINT specified in the DJANGO_SETTINGS_MODULE
    """

    #need a consumer key (settings_module.ITS_CONSUMER_KEY)
    #need a consumer secret (settings_module.ITS_CONSUMER_SECRET_KEY)
    #need an its endpoint (settings_module.ITS_ENDPOINT)
    conn = mkcon()
    conn.query("""
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
    pass





    #Migrate images


    # res1 =SELECT id from merlin.core_webobject where webobject_type = 'Video'
    # for id in res1:
    #
    #
    #

    #2.Migrate