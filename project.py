import sys
import os
from datetime import datetime
import csv

try:
    import mysql.connector
except ModuleNotFoundError:
    print("Fail")
    sys.exit(1)

# --- Database connection setup ---
DB_CONFIG = {
    'user': 'test',
    'password': 'password',
    'database': 'cs122a'
}

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def print_result(cursor):
    rows = cursor.fetchall()
    for row in rows:
        print(",".join(str(item) for item in row))

# --- 1. import ---
def recreate_tables(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        uid INT PRIMARY KEY,
        email TEXT NOT NULL,
        joined_date DATE NOT NULL,
        nickname TEXT NOT NULL,
        street TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        genres TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS producers (
        uid INT PRIMARY KEY,
        bio TEXT,
        company TEXT,
        FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS viewers (
        uid INT PRIMARY KEY,
        subscription ENUM('free', 'monthly', 'yearly'),
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS releases (
        rid INT PRIMARY KEY,
        producer_uid INT NOT NULL,
        title TEXT NOT NULL,
        genre TEXT NOT NULL,
        release_date DATE NOT NULL,
        FOREIGN KEY (producer_uid) REFERENCES producers(uid) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        rid INT,
        website_url TEXT,
        PRIMARY KEY (rid),
        FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series (
        rid INT,
        introduction TEXT,
        PRIMARY KEY (rid),
        FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        rid INT,
        ep_num INT NOT NULL,
        title TEXT NOT NULL,
        length INT NOT NULL,
        PRIMARY KEY (rid, ep_num),
        FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        sid INT PRIMARY KEY,
        uid INT NOT NULL,
        rid INT NOT NULL,
        ep_num INT NOT NULL,
        initiate_at DATETIME NOT NULL,
        leave_at DATETIME NOT NULL,
        quality ENUM('480p', '720p', '1080p'),
        device ENUM('mobile', 'desktop'),
        FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
        FOREIGN KEY (rid, ep_num) REFERENCES videos(rid, ep_num) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        rvid INT PRIMARY KEY,
        uid INT NOT NULL,
        rid INT NOT NULL,
        rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
        body TEXT,
        posted_at DATETIME NOT NULL,
        FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
        FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """)


# --- 1. import ---

import os
import csv
from datetime import datetime

def import_data(folder):
    try:
        db = connect_db()
        cursor = db.cursor()
        recreate_tables(cursor)

        # Order matters due to FK constraints
        table_insert_order = [
            "users", "producers", "viewers", "releases",
            "movies", "series", "videos", "sessions", "reviews"
        ]

        for table in table_insert_order:
            filepath = os.path.join(folder, f"{table}.csv")
            if not os.path.exists(filepath):
                print(f"File {filepath} not found, skipping.")
                continue

            with open(filepath, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip header row

                for row in reader:
                    if table == "users":
                        cursor.execute(
                            "INSERT INTO users (uid, email, joined_date, nickname, street, city, state, zip, genres) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))

                    elif table == "producers":
                        cursor.execute(
                            "INSERT INTO producers (uid, bio, company) VALUES (%s,%s,%s)", tuple(row))

                    elif table == "viewers":
                        cursor.execute(
                            "INSERT INTO viewers (uid, subscription, first_name, last_name) "
                            "VALUES (%s,%s,%s,%s)", tuple(row))

                    elif table == "releases":
                        cursor.execute(
                            "INSERT INTO releases (rid, producer_uid, title, genre, release_date) "
                            "VALUES (%s,%s,%s,%s,%s)", tuple(row))

                    elif table == "movies":
                        cursor.execute(
                            "INSERT INTO movies (rid, website_url) VALUES (%s,%s)", tuple(row))

                    elif table == "series":
                        cursor.execute(
                            "INSERT INTO series (rid, introduction) VALUES (%s,%s)", tuple(row))

                    elif table == "videos":
                        cursor.execute(
                            "INSERT INTO videos (rid, ep_num, title, length) VALUES (%s,%s,%s,%s)", tuple(row))

                    elif table == "sessions":
                        cursor.execute(
                            "INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))

                    elif table == "reviews":
                        cursor.execute(
                            "INSERT INTO reviews (rvid, uid, rid, rating, body, posted_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s)", tuple(row))

        db.commit()
        print("Success")

    except Exception as e:
        print("Fail")
        #print("Error:", e)
    finally:
        if cursor: cursor.close()
        if db: db.close()




def print_all_tables(cursor):
    table_names = [
        "users", "producers", "viewers", "releases",
        "movies", "series", "videos", "sessions", "reviews"
    ]
    for table in table_names:
        print(f"\n--- {table.upper()} ---")
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        for row in rows:
            print(",".join(str(item) for item in row))

# Example usage:
# import_data("/path/to/your/csv/folder")

# --- 2. insertViewer ---
def insert_viewer(args):
    try:
        db = connect_db()
        cursor = db.cursor()

        uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription = args
        cursor.execute("INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (uid, email, joined_date, nickname, street, city, state, zip_code, genres))
        cursor.execute("INSERT INTO viewers VALUES (%s, %s, %s, %s)", (uid, subscription, first, last))

        db.commit()
        print("Success")
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()

# --- 3. addGenre ---
def add_genre(uid, new_genre):
    try:
        uid = int(uid)
        new_genre = new_genre.strip().lower()  # Optional: normalize case


        db = connect_db()
        cursor = db.cursor()


        cursor.execute("SELECT genres FROM users WHERE uid = %s", (uid,))
        result = cursor.fetchone()


        if result is not None:
            current_genres = result[0] or ""
            genre_list = [g.strip().lower() for g in current_genres.split(';') if g.strip()]


            if new_genre in genre_list:
                print("Fail")  # Already exists
                return


            updated_genres = ";".join(genre_list + [new_genre])
            cursor.execute("UPDATE users SET genres = %s WHERE uid = %s", (updated_genres, uid))
            db.commit()
            print("Success")
        else:
            print("Fail")
    except Exception as e:
        print("Fail")
        # print("Error:", e)  # Uncomment for debugging
    finally:
        if cursor: cursor.close()
        if db: db.close()


# --- 4. deleteViewer ---
def delete_viewer(uid):
    try:
        db = connect_db()
        cursor = db.cursor()
        
        # Delete the viewer record first
        cursor.execute("DELETE FROM viewers WHERE uid = %s", (uid,))
        
        # Now delete the user record
        cursor.execute("DELETE FROM users WHERE uid = %s", (uid,))
        
        # Commit the transaction
        db.commit()
        
        print("Success")
    except Exception as e:
        # Catch specific exception and print error message for debugging
        print(f"Fail")
    finally:
        # Ensure the cursor and connection are closed
        cursor.close()
        db.close()


"""
Insert movie
Insert a new movie in the appropriate table(s). Assume that the corresponding Release record already exists.
Input:
python3 project.py insertMovie [rid:int] [website_url:str]

EXAMPLE: python3 project.py insertMovie 1 top-gun.com
Output:
	Boolean
"""
# --- 5. insertMovie ---
def insert_movie(rid, website_url):
    try:
        db = connect_db()
        cursor = db.cursor()

        # Check if a movie with this rid already exists
        cursor.execute("SELECT 1 FROM movies WHERE rid = %s", (rid,))
        result = cursor.fetchone()

        if result:
            print("Fail")  # Don't insert duplicate movies

        # Check if release exists before inserting movie (optional, since it's assumed)
        cursor.execute("SELECT 1 FROM releases WHERE rid = %s", (rid,))
        release_exists = cursor.fetchone()
        if not release_exists:
            return "Fail"  # Foreign key violation safety

        # Insert movie
        cursor.execute("INSERT INTO movies (rid, website_url) VALUES (%s, %s)", (rid, website_url))
        db.commit()
        print("Success")
    except mysql.connector.Error:
        print("Fail")
    finally:
        cursor.close()
        db.close()


def print_all_movies():
    try:
        db = connect_db()  # Connect to the database
        cursor = db.cursor()

        # Execute a SELECT query to fetch all data from the movies table
        cursor.execute("SELECT * FROM movies")

        # Fetch all rows from the query result
        rows = cursor.fetchall()

        # Loop through the rows and print each one
        for row in rows:
            print(row)

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()


# --- 6. insertSession ---
def insert_session(args):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("INSERT INTO sessions VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", args)
        db.commit()
        print("Success")
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()

# --- 7. updateRelease ---
def update_release(rid, title):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("UPDATE releases SET title = %s WHERE rid = %s", (title, rid))
        db.commit()
        print("Success")
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()

# --- 8. listReleases ---
def list_releases(uid):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT r.rid, r.genre, r.title
            FROM releases r
            JOIN reviews v ON r.rid = v.rid
            WHERE v.uid = %s
            GROUP BY r.rid, r.genre, r.title
            ORDER BY r.title ASC
        """, (uid,))
        print_result(cursor)
    except:
        return "Fail"
    finally:
        cursor.close()
        db.close()


# --- 9. popularRelease ---
def popular_release(N):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT r.rid, r.title, COUNT(*) AS reviewCount
            FROM reviews v
            JOIN releases r ON v.rid = r.rid
            GROUP BY r.rid
            ORDER BY reviewCount DESC, r.rid DESC
            LIMIT %s
        """, (N,))
        print_result(cursor)
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()

# --- 10. releaseTitle ---
def release_title(sid):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT r.rid, r.title, r.genre, v.title, v.ep_num, v.length
            FROM sessions s
            JOIN videos v ON s.rid = v.rid AND s.ep_num = v.ep_num
            JOIN releases r ON v.rid = r.rid
            WHERE s.sid = %s
            ORDER BY r.title ASC
        """, (sid,))
        print_result(cursor)
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()

# --- 11. activeViewer ---
def active_viewer(N, start, end):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT u.uid, v.firstname, v.lastname
            FROM sessions s
            JOIN viewers v ON s.uid = v.uid
            JOIN users u ON v.uid = u.uid
            WHERE DATE(s.initiate_at) BETWEEN %s AND %s
            GROUP BY s.uid
            HAVING COUNT(*) >= %s
            ORDER BY s.uid ASC
        """, (start, end, N))
        print_result(cursor)
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()

# --- 12. videosViewed ---
def videos_viewed(rid):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT v.rid, v.ep_num, v.title,
                   v.length,
                   IFNULL(vc.count, 0) AS viewer_count
            FROM videos v
            LEFT JOIN (
                SELECT rid, ep_num, COUNT(DISTINCT uid) AS count
                FROM sessions
                GROUP BY rid, ep_num
            ) vc
            ON v.rid = vc.rid AND v.ep_num = vc.ep_num
            WHERE v.rid = %s
            ORDER BY v.rid DESC, v.ep_num ASC
        """, (rid,))
        rows = cursor.fetchall()

        for row in rows:
            # Clean and format: [rid, ep_num, title, length, count]
            rid = str(int(row[0]))
            ep_num = str(int(row[1]))
            title = row[2].strip()
            length = str(int(row[3]))
            count = str(int(row[4]))
            print(",".join([rid, ep_num, title, length, count]))

    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()


# --- Main entry point ---
if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print("No command provided.")
        sys.exit(1)

    command = args[0]
    params = args[1:]

    if command == 'import':
        import_data(*params)
    elif command == 'insertViewer':
        insert_viewer(params)
    elif command == 'addGenre':
        add_genre(params[0], params[1])
    elif command == 'deleteViewer':
        delete_viewer(params[0])
    elif command == 'insertMovie':
        insert_movie(int(params[0]), params[1])
    elif command == 'insertSession':
        insert_session(params)
    elif command == 'updateRelease':
        update_release(params[0], params[1])
    elif command == 'listReleases':
        list_releases(params[0])
    elif command == 'popularRelease':
        popular_release(int(params[0]))
    elif command == 'releaseTitle':
        release_title(params[0])
    elif command == 'activeViewer':
        active_viewer(params[0], params[1], params[2])
    elif command == 'videosViewed':
        videos_viewed(params[0])
    else:
        print("Unknown command.")
