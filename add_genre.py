'''

Add genres
Add a new genre to a user. For purposes of this homework, the genres attribute is a list of semicolon separated words. If the user already has existing genres (e.g. "comedy;romance"), the new genre should be added to the semicolon-separated list (e.g. for a new genre "horror", the genre column will be updated to "comedy;romance;horror".

Input:
python3 project.py addGenre [uid:int] [genre:str]

EXAMPLE: python3 project.py addGenre 1 Comedy
Output:
	Boolean

'''

def add_genre(genre):
    # genre is a list of attr
    print()