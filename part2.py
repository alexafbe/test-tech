import pymysql

con = pymysql.connect(
    host="127.0.0.1",
    port=8889,
    user="root",
    passwd="root",
    db='testEnlaps'
)

curs = con.cursor()

#Select image where seq = seq (inner join ?)
curs.execute("SELECT sequence FROM sequences")
get_sequences = curs.fetchall()

for seq in get_sequences:
    curs.execute("SELECT * FROM images WHERE sequence = '" + seq[0] + "'")
    get_images = curs.fetchall()

    sides = []

    if len(get_images) == 2:
        for el in get_images:
            sides.append(el[1])
        if "left" in sides and "right" in sides and "stitched" not in sides:
            print("Sending sequence " + seq[0] + " to the stitching service")

    elif len(get_images) == 3:
        for el in get_images:
            sides.append(el[1])
        if "left" in sides and "right" in sides and "stitched" in sides:
            print("Sequence " + seq[0] + " already stitched")

    else:
        print("Error on image " + get_images[0][0])