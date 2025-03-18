'''Insert viewer
Insert a new viewer into the related tables. 

Input:
python3 project.py insertViewer [uid:int] [email:str] [nickname:str] [street:str] [city:str] 
[state:str] [zip:str] [genres:str] [joined_date:date] [first:str] [last:str] [subscription:str]

EXAMPLE: python3 project.py insertViewer 1 test@uci.edu awong "1111 1st street" Irvine 
CA 92616 "romance;comedy" 2020-04-19 Alice Wong yearly

Output:
	Boolean
'''

import csv

def insertUser(input_uid, input_email, input_nickname, input_street, input_city, input_state, input_zip, input_genres, input_date, input_fname, input_lname, input_sub):
    # write User: uid, email, jdate, nickname, street, city, state, zip, genres
    with open("output_user.csv", mode="w", newline="") as user_file:
        writer = csv.writer(user_file) 
        writer.writerow([input_uid, input_email, input_date, input_nickname, input_street, input_city, input_state, input_zip, input_genres]) # header row
    
    # writing viewer to viewer csv file
    viewer_path = r"C:\Users\konch\OneDrive\Documents\cs122a_final\test_data\viewers.csv"
    with open(viewer_path, 'a', newline='') as viewers_csv:
         csv_writer = csv.writer(viewers_csv)
         csv_writer.writerow([input_uid, input_sub, input_fname, input_lname])
    
    # writing user to user csv file
    user_path = r"C:\Users\konch\OneDrive\Documents\cs122a_final\test_data\users.csv"
    with open(user_path, 'a', newline='') as user_csv:
        csv_writer = csv.writer(user_csv)
        csv_writer.writerow([input_uid, input_email, input_date, input_nickname, input_street, input_city, input_state, input_zip, input_genres]) # header row

def deleteUser(uid_delete):
    viewer_path = r"C:\Users\konch\OneDrive\Documents\cs122a_final\test_data\viewers.csv"
    rows = []
    # open viewer file and read it to find matching uid line
    with open(viewer_path, mode='r', newline='') as file:
        reader = csv.reader(file)
        rows = list(reader)
    row_count = 0 # indexing through file
    for row in rows: # find the matching uid
        try:
            u_id = int(row[0])
            if u_id == uid_delete:
                print("deleting row with uid", uid_delete, "...")
                del rows[row_count]
        except Exception as e:
            pass 
        row_count += 1

    # rewrite to file
    with open(viewer_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)
        print("rewrote to viewer file...")


    # open user file and read to find matching uid line
    u_rows = []
    user_path = r"C:\Users\konch\OneDrive\Documents\cs122a_final\test_data\users.csv"
    with open(user_path, mode='r', newline='') as file:
        reader = csv.reader(file)
        u_rows = list(reader)
    u_row_count = 0 # indexing through file
    for row in u_rows: # find the matching uid
        try:
            u_id = int(row[0])
            if u_id == uid_delete:
                print("deleting row with uid", uid_delete, "...")
                del u_rows[u_row_count]
        except Exception as e:
            pass 
        u_row_count += 1

    # rewrite to user file
    with open(user_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(u_rows)
        print("rewrote to user file...")



        
