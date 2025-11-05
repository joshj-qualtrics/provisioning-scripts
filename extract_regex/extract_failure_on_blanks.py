import re

with open("results.txt") as f:
    t = f.read().strip()

# Product Code Failures to Blank product code values
# pattern = r"Some product code could not be parsed: ((?=[^.]*\s)[^.]+)\."

# All Product Code Failures
pattern = r"Some product code could not be parsed: ((?=[^.])[^.]+)\." 


matches = re.findall(pattern, t)

for code in matches:
    print(code)

print(f"\nTotal occurrences: {len(matches)}")