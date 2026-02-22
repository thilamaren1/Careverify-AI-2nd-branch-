import os
import re
import glob

html_dir = r"c:\Users\HP\Desktop\Careverify-AI-2nd-branch-\frontend"
files = glob.glob(os.path.join(html_dir, "*.html"))

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find where Risk Scoring ends
    match = re.search(r'(Risk Scoring\s*</a>)', content, re.IGNORECASE)
    if not match:
        continue
    
    start_idx = match.end()
    
    # Find the preservation point
    footer_match = re.search(r'(<div class="sidebar-footer">)', content[start_idx:], re.IGNORECASE)
    account_match = re.search(r'(<span class="sidebar-section">\s*Account\s*</span>)', content[start_idx:], re.IGNORECASE)
    aside_match = re.search(r'(</aside>)', content[start_idx:], re.IGNORECASE)
    
    end_idx = -1
    if footer_match:
        end_idx = start_idx + footer_match.start()
    elif account_match:
        end_idx = start_idx + account_match.start()
    elif aside_match:
        end_idx = start_idx + aside_match.start()
        
    if end_idx != -1:
        new_content = content[:start_idx] + "\n\n            " + content[end_idx:]
        if new_content != content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Updated {filepath}")
        else:
            print(f"No changes needed in {filepath}")
    else:
        print(f"Could not find end point in {filepath}")
