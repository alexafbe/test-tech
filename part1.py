import json, pymysql, jsonschema
from jsonschema import validate

jsonSchema = {
    "type": "object",
    "properties": {
        "s3_key": {"type" : "string"},
        "resolution": {"type" : "string"},
        "file_size": {"type" : "integer"},
        "shooting_date": {"type" : "string"},
        "metadata": {
            "GPSLatitude": {"type" : "number"},
            "GPSLongitude": {"type" : "number"},
            "GPSAltitude": {"type" : "number"},
            "Camera Model Name": {"type" : "string"},
            "Make": {"type" : "string"}
        }
    }
}

jsonFile = """{
    "s3_key": "677c082c-3a1a-44d9-874a-20169546c653/123456789/right/my_photo.jpg",
    "resolution": "4096x1862",
    "file_size": 456874,
    "shooting_date": "2021-07-16 11:33:10.592579",
    "metadata": {
        "GPSLatitude": 0.34,
        "GPSLongitude": 0.45,
        "GPSAltitude": 0.78,
        "Camera Model Name": "TIKEE",
        "Make": "ENLAPS"
    }
}
"""

loadedFile = json.loads(jsonFile)

con = pymysql.connect(
    host="127.0.0.1",
    port=8889,
    user="root",
    passwd="root",
    db='testEnlaps'
)

curs = con.cursor()

def validateJSON(jsonFile):
    try:
        validate(instance=jsonFile, schema=jsonSchema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True

def checkImgResolution(s3_key, img_attr, img_name, img_resolution, img_size, img_meta_GPS_lat, img_meta_GPS_long, img_meta_GPS_alt, img_meta_cam_model, img_meta_make, img_shoot_date, seq_id):
    
    if img_attr == 'left':
        curs.execute("SELECT resolution FROM images WHERE sequence = '" + seq_id + "' AND attribute = 'right'")
    elif img_attr == 'right':
        curs.execute("SELECT resolution FROM images WHERE sequence = '" + seq_id + "' AND attribute = 'left'")
    
    get_img_res = curs.fetchone()

    if get_img_res == None or get_img_res[0] == img_resolution:
        insert_img_db = "INSERT INTO images (s3_key, attribute, file_name, resolution, file_size, GPSLatitude, GPSLongitude, GPSAltitude, camera_model, make, shooting_date, sequence) VALUES ('" + s3_key + "', '" + img_attr + "', '" + img_name + "', '" + img_resolution + "', '" + str(img_size) + "', '" + str(img_meta_GPS_lat) + "', '" + str(img_meta_GPS_long) + "', '" + str(img_meta_GPS_alt) + "', '" + img_meta_cam_model + "', '" + img_meta_make + "', '" + img_shoot_date + "', '" + seq_id + "')"
        curs.execute(insert_img_db)
        return True
    elif get_img_res[0] != img_resolution:
        curs.execute("DELETE FROM sequences WHERE sequence = '" + seq_id + "'")
        return False

def getData(json):
    if validateJSON(json):
        split_s3key = json['s3_key'].split('/')

        #s3_key elements
        tikee_uuid = split_s3key[0]
        seq_id = split_s3key[1]
        img_attr = split_s3key[2]
        img_name = split_s3key[3]

        #image data elements
        img_s3_key = json['s3_key']
        img_resolution = json['resolution']
        img_size = json['file_size']
        img_shoot_date = json['shooting_date']
        img_meta_GPS_lat = json['metadata']['GPSLatitude']
        img_meta_GPS_long = json['metadata']['GPSLongitude']
        img_meta_GPS_alt = json['metadata']['GPSAltitude']
        img_meta_cam_model = json['metadata']['Camera Model Name']
        img_meta_make = json['metadata']['Make']

        #Does the sequence exist
        curs.execute("SELECT * FROM sequences WHERE sequence = '" + seq_id + "'")
        is_seq_in = curs.fetchone()
        #Create it if not
        if is_seq_in == None:
            curs.execute("INSERT INTO sequences (sequence, tikee_uuid) VALUES ('" + seq_id + "', '" + tikee_uuid + "')")
        
        check_res = checkImgResolution(img_s3_key, img_attr, img_name, img_resolution, img_size, img_meta_GPS_lat, img_meta_GPS_long, img_meta_GPS_alt, img_meta_cam_model, img_meta_make, img_shoot_date, seq_id)
        
        if not check_res:
            print(json['s3_key'] + ' sequence removed (resolution not ok)')
        
        con.commit()
    else:
        print(json['s3_key'] + ' JSON format validation error')

getData(loadedFile)