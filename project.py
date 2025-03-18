#file imports
import re
import import_table 
import viewer_actions

# take an input statement like a command line, and read it


def parse_command(command):
    # Regular expression to match quoted strings or non-space sequences
    pattern = r'"([^"]+)"|(\S+)'
    
    # Find all matches and extract the correct group (quoted or unquoted)
    matches = re.findall(pattern, command)
    
    # Process extracted parts
    processed_parts = []
    for quoted, unquoted in matches:
        part = quoted if quoted else unquoted  # Pick non-empty match
        
        # Convert to int if it's numeric
        if part.isdigit():
            processed_parts.append(int(part))
        # Convert genre field (semicolon-separated) into a semicolon string
        elif ';' in part:
            processed_parts.append(part)  # Keep it as the same semicolon-separated string
        else:
            processed_parts.append(part)

    return processed_parts

def run_commands(command):
    if command[2] == "import":
        import_table.read_directory_files(command[3])

    elif command[2] == "insertViewer":
        print("inserting viewer data...")
        viewer_actions.insertUser(command[3], command[4], command[5], command[6], command[7], command[8], command[9], command[10], command[11], command[12], command[13], command[14].strip("")) 
        print("wrote to user and viewer files")
    elif command[2] == "deleteViewer":
        print("deleting viewer")
        print(int(command[3]))
        viewer_actions.deleteUser(int(command[3]))



# Example usage
# command = '
# python3 project.py insertViewer 12 kkonchad@uci.edu kkonchad "118 Desert Bloom" Irvine CA 92618 "romance;comedy;action;horror" 2025-03-16 Krisha Konchadi monthly
# python3 project.py insertViewer 27 ssvasams@uci.edu ssvasams "53 Calypso" Irvine CA 92618 "thriller;romance;comedy;horror;kdrama" 2025-03-16 Soumili Vasamsetty yearly
# python3 project.py insertViewer 1 test@uci.edu awong "1111 1st street" Irvine CA 92616 "romance;comedy" 2020-04-19 Alice Wong yearly
# parsed = parse_command(command)

# print(parsed)


if __name__ == "__main__":
    comm = input()
    parsed = parse_command(comm) # gets list of commands
    # print(parsed)
    run_commands(parsed) # will do the functions


# python3 project.py import test_data
# python3 project.py insertViewer 27 ssvasams@uci.edu ssvasams "53 Calypso" Irvine CA 92618 "thriller;romance;comedy;horror;kdrama" 2025-03-16 Soumili Vasamsetty yearly
# h