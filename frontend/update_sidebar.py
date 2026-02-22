import os
import re
import glob

html_dir = r"c:\Users\HP\Desktop\Careverify-AI-2nd-branch-\frontend"
files = glob.glob(os.path.join(html_dir, "*.html"))

pattern = re.compile(r'(Risk Scoring\s*</a>).*?(<div class="sidebar-footer">)', re.DOTALL)

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content, num_subs = pattern.subn(r'\1\n\n            \2', content)

    if num_subs > 0:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filepath}")
    else:
        print(f"No match found in {filepath}")
