'''
Delete existing tables, and create new tables. Then read the csv files in the given folder and import data into the database.
 You can assume that the folder always contains all the necessary CSV files and the files are correct. 

 Input:
python3 project.py import [folderName:str]

python3 project.py import test_data
Output:
	Boolean


'''

import os

# imports a folder
def import_folder(folder_name):
    try:
        folder_object = os.listdir(folder_name) # opens folder given from command
        for file_name in folder_object:
            file_path = os.path.join(folder_object, file_name) # gets indiv file path
            with open(file_path, 'r') as file:
                file_contents = file.read()
                print(f"Contents of {file_name}: \n{file_contents}\n")
        print("Success")
    except FileNotFoundError:
        print(f"Error: Directory '{folder_name}' not found.")
        print("Fail")
    except NotADirectoryError:
        print(f"Error: '{folder_name}' is not a directory.")
        print("Fail")
    except Exception as e:
       print(f"An error occurred: {e}") # print out exceptions
       print("Fail")


def read_directory_files(dir_path):
    try:
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            if os.path.isfile(file_path) and "csv" in file_path: # Check if it is a file
                with open(file_path, 'r') as file:
                    contents = file.read()
                    print(f"Contents of {filename}:\n{contents}\n")
            else:
                print(f"{filename} is not a file.")
    except FileNotFoundError:
        print(f"Error: Directory '{dir_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage:
# directory_path = "path/to/your/directory" # Replace with the actual path
# read_directory_files(directory_path)
