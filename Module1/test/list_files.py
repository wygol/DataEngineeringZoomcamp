from pathlib import Path


# this is similar to os.path. ...
# here we just dissect the absolute path up to folder and then the filename 
current_dir = Path.cwd() 
current_file = Path(__file__).name

print("Files in {0}:".format(current_dir))

for filepath in current_dir.iterdir():
    # if the script sees itself, i.e list_files.py == list_files.py 
    # because iterdir will give all files in the dir to the function
    # [file1.txt, file2.txt, file3.txt, list_files.py]

    if filepath.name == current_file:
        continue
    
    print(f"  - {filepath.name}")
    
    if filepath.is_file():
        content = filepath.read_text(encoding="utf-8")
        print(f"    Content: {content}")