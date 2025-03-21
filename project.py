import sys
from datetime import datetime


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


# Recreate tables (add schema here as per your needs)
def recreate_tables(cursor):
    # Example: users table creation
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS producers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        country VARCHAR(255)
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS viewers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS releases (
        id INT AUTO_INCREMENT PRIMARY KEY,
        movie_id INT,
        release_date DATE,
        FOREIGN KEY (movie_id) REFERENCES movies(id)
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        genre VARCHAR(255)
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        genre VARCHAR(255)
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        length INT
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        session_start TIMESTAMP,
        session_end TIMESTAMP
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INT AUTO_INCREMENT PRIMARY KEY,
        reviewer_id INT,
        movie_id INT,
        review_text TEXT,
        FOREIGN KEY (reviewer_id) REFERENCES viewers(id),
        FOREIGN KEY (movie_id) REFERENCES movies(id)
    );
    """)


# Import data from CSV and insert manually
def import_data(folder):
    try:
        db = connect_db()  # Connect to the database
        cursor = db.cursor()


        # Step 1: Clear the existing data from all tables
        tables = ["users", "producers", "viewers", "releases", "movies", "series", "videos", "sessions", "reviews"]
       
        for table in tables:
            cursor.execute(f"DELETE FROM {table};")  # Delete data from each table
       
        # Step 2: Recreate the tables
        recreate_tables(cursor)  # Recreate tables with DDL statements
       
        # Step 3: Loop through tables and import data from CSV files
        for table in tables:
            filepath = os.path.join(folder, f"{table}.csv")
           
            # Check if the file exists
            if not os.path.exists(filepath):
                print(f"Warning: File {filepath} not found. Skipping this table.")
                continue  # Skip this table if the file doesn't exist


            # Open the CSV file and read its rows
            with open(filepath, newline='', encoding='utf-8') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Skip the header row
               
                for row in csvreader:
                    # Build the INSERT query for the current table
                    if table == "users":
                        insert_query = """
                        INSERT INTO users (name, email)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "producers":
                        insert_query = """
                        INSERT INTO producers (name, country)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "viewers":
                        insert_query = """
                        INSERT INTO viewers (name, email)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "releases":
                        insert_query = """
                        INSERT INTO releases (movie_id, release_date)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "movies":
                        insert_query = """
                        INSERT INTO movies (title, genre)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "series":
                        insert_query = """
                        INSERT INTO series (title, genre)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "videos":
                        insert_query = """
                        INSERT INTO videos (title, length)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "sessions":
                        insert_query = """
                        INSERT INTO sessions (session_start, session_end)
                        VALUES (%s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1]))
                   
                    elif table == "reviews":
                        insert_query = """
                        INSERT INTO reviews (reviewer_id, movie_id, review_text)
                        VALUES (%s, %s, %s);
                        """
                        cursor.execute(insert_query, (row[0], row[1], row[2]))


                db.commit()  # Commit after inserting data for each table
                print(f"Data loaded into {table} successfully.")


        print("Data import completed successfully.")


    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
    finally:
        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()




# Example usage:
# import_data("/path/to/your/csv/folder")


# --- 2. insertViewer ---
def insert_viewer(args):
    try:
        db = connect_db()
        cursor = db.cursor()


        uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription = args
        cursor.execute("INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (uid, email, nickname, street, city, state, zip_code, genres, joined_date))
        cursor.execute("INSERT INTO viewers VALUES (%s, %s, %s, %s)", (uid, first, last, subscription))


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
        db = connect_db()
        cursor = db.cursor()


        cursor.execute("SELECT genres FROM users WHERE uid = %s", (uid,))
        result = cursor.fetchone()
        if result:
            current_genres = result[0]
            updated = f"{current_genres};{new_genre}" if current_genres else new_genre
            cursor.execute("UPDATE users SET genres = %s WHERE uid = %s", (updated, uid))
            db.commit()
            print("Success")
        else:
            print("Fail")
    except:
        print("Fail")
    finally:
        cursor.close()
        db.close()


# --- 4. deleteViewer ---
def delete_viewer(uid):
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("DELETE FROM viewers WHERE uid = %s", (uid,))
        cursor.execute("DELETE FROM users WHERE uid = %s", (uid,))
        db.commit()
        print("Success")
    except:
        print("Fail")
    finally:
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
def insert_movie(rid, website_url):


    # Call the function to print all movies
    print_all_movies()
    #only insert should the rid not alr exist!
    try:
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("INSERT INTO movies VALUES (%s, %s)", (rid, website_url))
        db.commit()
        return("Success")
    except:
        return("Fail")
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
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM releases r
            JOIN reviews v ON r.rid = v.rid
            WHERE v.uid = %s
            ORDER BY r.title ASC
        """, (uid,))
        print_result(cursor)
    except:
        print("Fail")
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
            SELECT v.rid, v.ep_num, v.title, v.length, COUNT(DISTINCT s.uid)
            FROM videos v
            LEFT JOIN sessions s ON v.rid = s.rid AND v.ep_num = s.ep_num
            WHERE v.rid = %s
            GROUP BY v.rid, v.ep_num
            ORDER BY v.rid DESC
        """, (rid,))
        print_result(cursor)
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
        insert_movie(params[0], params[1])
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
